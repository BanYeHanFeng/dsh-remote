#!/bin/sh
# dsh-remote 三端管理脚本（POSIX sh）
# 用法：./dsh-remote.sh <hub|agent|server> <action>
# 无参数进入交互菜单；action 见 --help。

set -u
umask 077

REPO=${REPO:-BanYeHanFeng/dsh-remote}
PROGRAM=dsh-remote
BIN=${DSH_REMOTE_BIN:-/usr/local/bin/dsh-remote}
CONF_DIR=${DSH_REMOTE_CONF_DIR:-/etc/dsh-remote}
STATE_DIR=${DSH_REMOTE_STATE_DIR:-/var/lib/dsh-remote}
LOG_DIR=${DSH_REMOTE_LOG_DIR:-/var/log/dsh-remote}
SYSTEMD_DIR=${DSH_REMOTE_SYSTEMD_DIR:-/etc/systemd/system}
SYSTEMD_PREFIX=${DSH_REMOTE_SYSTEMD_PREFIX:-dsh-remote}
DEFAULT_HUB=${DSH_REMOTE_DEFAULT_HUB:-}
DEFAULT_NAME=${DSH_REMOTE_DEFAULT_NAME:-$(uname -n 2>/dev/null || printf 'dsh-remote')}
DEFAULT_ROOTS=${DSH_REMOTE_DEFAULT_ROOTS:-/data}
DEFAULT_LISTEN=${DSH_REMOTE_DEFAULT_LISTEN:-0.0.0.0:7000}
DEFAULT_MAX_OUTPUT=${DSH_REMOTE_DEFAULT_MAX_OUTPUT:-262144}
DEFAULT_MAX_FILE=${DSH_REMOTE_DEFAULT_MAX_FILE:-67108864}
DEFAULT_CHANNEL=${DSH_REMOTE_DEFAULT_CHANNEL:-stable}
DEV_TAG=${DSH_REMOTE_DEV_TAG:-}
DEV_TAG_PREFIX=${DSH_REMOTE_DEV_TAG_PREFIX:-rolling-}
CHANNEL_FILE=${DSH_REMOTE_CHANNEL_FILE:-$STATE_DIR/channel}

TMP_DIRS=
ECHO_OFF=0
STTY_STATE=
ASK_RESULT=
FETCH_BIN=
FETCH_VERSION=
CHANNEL=

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    C_INFO=$(printf '\033[0;32m')
    C_WARN=$(printf '\033[1;33m')
    C_ERR=$(printf '\033[0;31m')
    C_OFF=$(printf '\033[0m')
else
    C_INFO=
    C_WARN=
    C_ERR=
    C_OFF=
fi

info() { printf '%s[信息]%s %s\n' "$C_INFO" "$C_OFF" "$*"; }
warn() { printf '%s[警告]%s %s\n' "$C_WARN" "$C_OFF" "$*" >&2; }
error() { printf '%s[错误]%s %s\n' "$C_ERR" "$C_OFF" "$*" >&2; }

usage() {
    cat <<EOF
用法:
  $0 <hub|agent|server> <action> [参数]
  $0 channel [stable|dev]     # 查看或切换发布渠道
  $0                          # 交互菜单

发布渠道:
  stable  正式版，取 GitHub Releases 的 latest（默认）
  dev     预发布版，取最新的滚动构建（tag 以 ${DEV_TAG_PREFIX}日期-哈希 命名）
  渠道记忆在 $CHANNEL_FILE，install/update 时按渠道下载

action:
  install     下载缺失的二进制（可选发布渠道）、交互写配置、安装并启动服务
  start       启动服务（server 由 DSH 拉起，只做提示）
  stop        停止服务
  restart     重启服务
  status      查看状态与最近日志
  update      按当前渠道下载最新 Release，原子替换并重启
  logs [-f] [行数]
              查看日志；-f 持续跟踪，默认 100 行
  doctor      检查二进制、配置、权限与中继连通性
  uninstall   停止并删除 unit；配置与 Key 默认保留

环境变量（install 非交互时使用）:
  DSH_REMOTE_HUB, DSH_REMOTE_KEY, DSH_REMOTE_NAME, DSH_REMOTE_ROOTS,
  DSH_REMOTE_LISTEN, DSH_REMOTE_ALLOW_BASH, DSH_REMOTE_MAX_OUTPUT, DSH_REMOTE_MAX_FILE
  DSH_REMOTE_ASSUME_YES=1  不再询问，直接采用上面的值或默认值
  DSH_REMOTE_BIN, DSH_REMOTE_CONF_DIR, DSH_REMOTE_STATE_DIR, DSH_REMOTE_LOG_DIR
  DSH_REMOTE_CHANNEL=stable|dev  发布渠道（默认 stable）
  DSH_REMOTE_CHANNEL_FILE        渠道状态文件（默认 $STATE_DIR/channel）
  DSH_REMOTE_DEV_TAG             固定某个滚动构建 tag（默认自动取最新 ${DEV_TAG_PREFIX}*）
  DSH_REMOTE_DOWNLOAD_BASE Release 下载目录（默认 GitHub latest/download 或 dev 渠道目录）
  DSH_REMOTE_FORCE=1        update 时同版本也重装
  DSH_REMOTE_NO_SYSTEMD=1   强制使用 PID 文件管理

配置目录: $CONF_DIR    二进制: $BIN
EOF
}

cleanup() {
    restore_tty
    if [ -n "$TMP_DIRS" ]; then
        old_ifs=$IFS
        IFS='
'
        for dir in $TMP_DIRS; do
            [ -n "$dir" ] && rm -rf "$dir"
        done
        IFS=$old_ifs
    fi
}

add_tmp() {
    if [ -z "$TMP_DIRS" ]; then
        TMP_DIRS=$1
    else
        TMP_DIRS="$TMP_DIRS
$1"
    fi
}

restore_tty() {
    if [ "$ECHO_OFF" = 1 ]; then
        if [ -n "$STTY_STATE" ]; then
            stty "$STTY_STATE" 2>/dev/null || stty echo 2>/dev/null || true
        else
            stty echo 2>/dev/null || true
        fi
        ECHO_OFF=0
    fi
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

need_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        error "缺少必需命令: $1"
        return 1
    fi
}

check_cmds() {
    missing=
    for cmd in "$@"; do
        command -v "$cmd" >/dev/null 2>&1 || missing="$missing $cmd"
    done
    if [ -n "$missing" ]; then
        error "缺少必需命令:$missing"
        return 1
    fi
}

check_base() {
    check_cmds uname id sed awk grep head tail tr sleep mktemp rm mkdir chmod cp mv readlink
}

systemd_available() {
    case "${DSH_REMOTE_NO_SYSTEMD:-0}" in
        1|yes|true) return 1 ;;
    esac
    case "${DSH_REMOTE_SYSTEMD:-0}" in
        1|yes|true) return 0 ;;
    esac
    command -v systemctl >/dev/null 2>&1 || return 1
    [ -d /run/systemd/system ] || return 1
    return 0
}

service_name() {
    printf '%s-%s.service\n' "$SYSTEMD_PREFIX" "$1"
}

unit_path() {
    printf '%s/%s\n' "$SYSTEMD_DIR" "$(service_name "$1")"
}

conf_path() {
    printf '%s/%s.conf\n' "$CONF_DIR" "$1"
}

pid_path() {
    printf '%s/%s.pid\n' "$STATE_DIR" "$1"
}

role_managed() {
    systemd_available || return 1
    [ -f "$(unit_path "$1")" ]
}

require_root() {
    [ "$(id -u)" -eq 0 ] && return 0
    if [ "${DSH_REMOTE_ALLOW_NONROOT:-0}" = 1 ]; then
        warn "以非 root 运行，可能无法写入 $BIN 或 $CONF_DIR"
        return 0
    fi
    if [ "${DSH_REMOTE_REEXEC:-0}" = 1 ]; then
        error "需要 root 权限"
        return 1
    fi
    if command -v sudo >/dev/null 2>&1; then
        info "该操作需要 root 权限，正在通过 sudo 重新执行..."
        DSH_REMOTE_REEXEC=1 exec sudo -E "$0" "$@"
    elif command -v su >/dev/null 2>&1; then
        info "该操作需要 root 权限，正在通过 su 重新执行..."
        DSH_REMOTE_REEXEC=1 exec su root -c 'exec "$@"' sh "$0" "$@"
    fi
    error "需要 root 权限，请用 sudo 运行: sudo $0 $*"
    return 1
}

conf_get() {
    file=$1
    key=$2
    [ -r "$file" ] || return 0
    sed -n "s/^[[:space:]]*${key}[[:space:]]*=[[:space:]]*//p" "$file" \
        | head -n 1 | sed 's/[[:space:]]*$//'
}

ask() {
    label=$1
    default=${2:-}
    ASK_RESULT=
    if [ -n "$default" ]; then
        printf '%s [%s]: ' "$label" "$default"
    else
        printf '%s: ' "$label"
    fi
    IFS= read -r ASK_RESULT || ASK_RESULT=
    if [ -z "$ASK_RESULT" ]; then
        ASK_RESULT=$default
    fi
}

ask_value() {
    if [ "${DSH_REMOTE_ASSUME_YES:-0}" = 1 ]; then
        ASK_RESULT=${2:-}
        return 0
    fi
    ask "$1" "$2"
}

ask_secret() {
    label=$1
    default=${2:-}
    hint=$3
    ASK_RESULT=$default
    if [ "${DSH_REMOTE_ASSUME_YES:-0}" = 1 ]; then
        return 0
    fi
    if [ -n "$hint" ]; then
        printf '%s（%s）: ' "$label" "$hint"
    else
        printf '%s: ' "$label"
    fi
    typed=
    hidden=0
    if [ -t 0 ] && command -v stty >/dev/null 2>&1; then
        STTY_STATE=$(stty -g 2>/dev/null || true)
        if stty -echo 2>/dev/null; then
            ECHO_OFF=1
            hidden=1
        fi
    fi
    IFS= read -r typed || typed=
    restore_tty
    if [ -n "$typed" ]; then
        ASK_RESULT=$typed
    fi
    if [ "$hidden" = 1 ]; then
        printf '\n'
    fi
}

confirm() {
    label=$1
    default=${2:-n}
    if [ "${DSH_REMOTE_ASSUME_YES:-0}" = 1 ]; then
        [ "$default" = y ] && return 0
        return 1
    fi
    printf '%s [%s]: ' "$label" "$default"
    answer=
    IFS= read -r answer || answer=
    if [ -z "$answer" ]; then
        answer=$default
    fi
    case "$answer" in
        y|Y|yes|YES|Yes|是) return 0 ;;
        *) return 1 ;;
    esac
}

validate_key() {
    key=$1
    case "$key" in
        '')
            error "Key 不能为空"
            return 1
            ;;
        *[[:space:]]*)
            error "Key 不能包含空白字符"
            return 1
            ;;
    esac
    if [ "${#key}" -lt 16 ]; then
        error "Key 至少需要 16 个字符（建议用 $PROGRAM genkey 生成）"
        return 1
    fi
    case "$key" in
        *[!A-Za-z0-9_+/=-]*)
            error "Key 含非法字符，只支持 base64 或 hex"
            return 1
            ;;
    esac
}

validate_hub() {
    hub=$1
    case "$hub" in
        '')
            error "中继地址不能为空"
            return 1
            ;;
        *[[:space:]]*)
            error "中继地址不能包含空白字符"
            return 1
            ;;
        *:*:*) : ;;
        *:*) : ;;
        *)
            error "中继地址需要 IP:端口，例如 1.2.3.4:7000"
            return 1
            ;;
    esac
    port=${hub##*:}
    case "$port" in
        ''|*[!0-9]*)
            error "中继地址端口非法: $hub"
            return 1
            ;;
    esac
    if [ "$port" -lt 1 ] || [ "$port" -gt 65535 ]; then
        error "中继地址端口超出范围: $hub"
        return 1
    fi
}

validate_listen() {
    listen=$1
    case "$listen" in
        '')
            error "监听地址不能为空"
            return 1
            ;;
        *[[:space:]]*)
            error "监听地址不能包含空白字符"
            return 1
            ;;
        *:*:*) : ;;
        *:*) : ;;
        *)
            error "监听地址需要 IP:端口，例如 0.0.0.0:7000"
            return 1
            ;;
    esac
    port=${listen##*:}
    case "$port" in
        ''|*[!0-9]*)
            error "监听地址端口非法: $listen"
            return 1
            ;;
    esac
    if [ "$port" -gt 65535 ]; then
        error "监听地址端口超出范围: $listen"
        return 1
    fi
}

validate_num() {
    field=$1
    number=$2
    case "$number" in
        ''|*[!0-9]*)
            error "$field 需要是非负整数，当前为: $number"
            return 1
            ;;
    esac
}

validate_allow_bash() {
    case "$1" in
        0|1|true|false) return 0 ;;
        *)
            error "ALLOW_BASH 只接受 0/1/true/false"
            return 1
            ;;
    esac
}

validate_roots() {
    roots=$1
    case "$roots" in
        /*) : ;;
        *)
            error "ROOTS 需要绝对路径，当前为: $roots"
            return 1
            ;;
    esac
    old_ifs=$IFS
    IFS=:
    for root in $roots; do
        case "$root" in
            '')
                IFS=$old_ifs
                error "ROOTS 含空目录项"
                return 1
                ;;
            /*) : ;;
            *)
                IFS=$old_ifs
                error "ROOTS 需要绝对路径，当前为: $root"
                return 1
                ;;
        esac
    done
    IFS=$old_ifs
}

detect_arch() {
    machine=$(uname -m 2>/dev/null || true)
    case "$machine" in
        x86_64|amd64) printf 'amd64\n' ;;
        aarch64|arm64) printf 'arm64\n' ;;
        *)
            error "不支持的架构: ${machine:-未知}（只发布 amd64 与 arm64）"
            return 1
            ;;
    esac
}

normalize_channel() {
    case "$1" in
        ''|stable|release|latest|正式版|正式) printf 'stable\n' ;;
        dev|prerelease|pre|beta|预发布版|预发布|测试版) printf 'dev\n' ;;
        *) return 1 ;;
    esac
}

channel_label() {
    case "${1:-}" in
        stable) printf '正式版' ;;
        dev) printf '预发布版' ;;
        '') printf '未设置' ;;
        *) printf '未知（%s）' "$1" ;;
    esac
}

dev_label() {
    if [ -n "$DEV_TAG" ]; then
        printf '固定 tag %s' "$DEV_TAG"
    else
        printf '自动取最新 %s*' "$DEV_TAG_PREFIX"
    fi
}

channel_saved() {
    [ -r "$CHANNEL_FILE" ] || return 0
    head -n 1 "$CHANNEL_FILE" 2>/dev/null | tr -d ' \t\r'
}

channel_persist() {
    want=$1
    chan=$(normalize_channel "$want") || {
        error "未知发布渠道: $want（可选 stable 正式版 / dev 预发布版）"
        return 1
    }
    mkdir -p "$STATE_DIR" || {
        error "无法创建目录: $STATE_DIR"
        return 1
    }
    file=$CHANNEL_FILE
    tmp="$file.tmp.$$"
    if printf '%s\n' "$chan" >"$tmp" && chmod 644 "$tmp" && mv -f "$tmp" "$file"; then
        return 0
    fi
    rm -f "$tmp"
    error "无法写入渠道文件: $file"
    return 1
}

resolve_channel() {
    channel_raw=${DSH_REMOTE_CHANNEL:-}
    [ -n "$channel_raw" ] || channel_raw=$(channel_saved)
    [ -n "$channel_raw" ] || channel_raw=$DEFAULT_CHANNEL
    CHANNEL=$(normalize_channel "$channel_raw") || {
        CHANNEL=
        error "未知发布渠道: $channel_raw（可选 stable 正式版 / dev 预发布版）"
        return 1
    }
}

channel_state() {
    if resolve_channel 2>/dev/null; then
        channel_label "$CHANNEL"
    else
        channel_label "无效：${DSH_REMOTE_CHANNEL:-未知}"
    fi
}

channel_choose() {
    resolve_channel || return 1
    if [ "${DSH_REMOTE_ASSUME_YES:-0}" = 1 ] || [ -n "${DSH_REMOTE_CHANNEL:-}" ]; then
        return 0
    fi
    info "发布渠道: 1) 正式版 stable  2) 预发布版 dev（$(dev_label)）"
    info "当前渠道: $(channel_label "$CHANNEL")"
    channel_tries=0
    while [ "$channel_tries" -lt 3 ]; do
        ask "选择发布渠道 [1/2]" "$CHANNEL"
        channel_ans=$ASK_RESULT
        case "$channel_ans" in
            1) channel_ans=stable ;;
            2) channel_ans=dev ;;
        esac
        if channel_norm=$(normalize_channel "$channel_ans"); then
            CHANNEL=$channel_norm
            return 0
        fi
        error "无效的发布渠道: $channel_ans（请输入 1、2、stable 或 dev）"
        channel_tries=$((channel_tries + 1))
    done
    error "发布渠道选择失败"
    return 1
}

release_base() {
    if [ -n "${DSH_REMOTE_DOWNLOAD_BASE:-}" ]; then
        printf '%s\n' "$DSH_REMOTE_DOWNLOAD_BASE"
        return 0
    fi
    case "$CHANNEL" in
        dev)
            _tag=${1:-$DEV_TAG}
            [ -n "$_tag" ] || return 1
            printf 'https://github.com/%s/releases/download/%s\n' "$REPO" "$_tag"
            ;;
        *) printf 'https://github.com/%s/releases/latest/download\n' "$REPO" ;;
    esac
}

tag_from_json() {
    sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p'
}

latest_tag() {
    if [ -n "${DSH_REMOTE_DOWNLOAD_BASE:-}" ]; then
        return 0
    fi
    command -v curl >/dev/null 2>&1 || return 0
    api=https://api.github.com/repos/$REPO/releases
    case "$CHANNEL" in
        dev)
            if [ -n "$DEV_TAG" ]; then
                curl -fsSL --max-time 5 "$api/tags/$DEV_TAG" 2>/dev/null | tag_from_json | head -n 1
            else
                curl -fsSL --max-time 5 "$api?per_page=50" 2>/dev/null \
                    | tag_from_json \
                    | grep "^$DEV_TAG_PREFIX" \
                    | head -n 1
            fi
            ;;
        *) curl -fsSL --max-time 5 "$api/latest" 2>/dev/null | tag_from_json | head -n 1 ;;
    esac
}

fetch_release() {
    check_cmds curl awk head chmod
    arch=$(detect_arch) || return 1
    [ -n "$CHANNEL" ] || resolve_channel || return 1
    info "发布渠道: $(channel_label "$CHANNEL")"
    asset="dsh-remote-linux-${arch}"
    tag=$(latest_tag)
    if [ "$CHANNEL" = dev ] && [ -z "$DEV_TAG" ] && [ -z "${DSH_REMOTE_DOWNLOAD_BASE:-}" ] && [ -z "$tag" ]; then
        error "拿不到滚动构建列表（GitHub API 限流？）"
        error "可设置 DSH_REMOTE_DEV_TAG 指定 tag，或 DSH_REMOTE_DOWNLOAD_BASE 指定下载目录"
        return 1
    fi
    base=$(release_base "$tag") || {
        error "无法确定滚动构建下载地址"
        return 1
    }
    if [ -n "$tag" ]; then
        info "最新 Release: $tag"
    fi
    info "下载 $base/$asset"
    tmp=$(mktemp -d "${TMPDIR:-/tmp}/dsh-remote.XXXXXX") || {
        error "mktemp -d 失败"
        return 1
    }
    add_tmp "$tmp"
    candidate=$tmp/$asset
    if ! curl -fsSL --connect-timeout 10 --retry 3 --retry-delay 2 -o "$candidate" "$base/$asset"; then
        error "下载失败: $base/$asset"
        error "检查网络、REPO=$REPO 或设置 DSH_REMOTE_DOWNLOAD_BASE"
        return 1
    fi
    if [ ! -x "$candidate" ]; then
        chmod 755 "$candidate" || {
            error "无法设置可执行权限: $candidate"
            return 1
        }
    fi
    version=$("$candidate" --version 2>/dev/null | awk '{print $2}')
    if [ -z "$version" ]; then
        error "下载的二进制无法在当前系统运行（架构不匹配？）"
        return 1
    fi
    FETCH_BIN=$candidate
    FETCH_VERSION=$version
    info "已下载 $PROGRAM $version"
}

install_candidate() {
    candidate=$1
    dest_dir=${BIN%/*}
    if [ "$dest_dir" = "$BIN" ]; then
        dest_dir=.
    fi
    mkdir -p "$dest_dir" || {
        error "无法创建目录: $dest_dir"
        return 1
    }
    tmp="$dest_dir/.$PROGRAM.new.$$"
    if ! cp "$candidate" "$tmp"; then
        error "复制到 $dest_dir 失败"
        rm -f "$tmp"
        return 1
    fi
    chmod 755 "$tmp" || {
        rm -f "$tmp"
        error "chmod 失败"
        return 1
    }
    if ! "$tmp" --version >/dev/null 2>&1; then
        rm -f "$tmp"
        error "新二进制无法在当前系统运行"
        return 1
    fi
    if ! mv -f "$tmp" "$BIN"; then
        rm -f "$tmp"
        error "原子替换失败: $BIN"
        return 1
    fi
}

bin_version() {
    [ -x "$BIN" ] || return 0
    "$BIN" --version 2>/dev/null | awk '{print $2}'
}

write_config() {
    role=$1
    conf=$(conf_path "$role")
    existing_key=$(conf_get "$conf" KEY)
    generated=0

    hub=
    key=${DSH_REMOTE_KEY:-}
    name=
    roots=
    allow_bash=
    listen=
    max_output=
    max_file=

    case "$role" in
        hub)
            listen=${DSH_REMOTE_LISTEN:-}
            [ -n "$listen" ] || listen=$(conf_get "$conf" LISTEN)
            [ -n "$listen" ] || listen=$DEFAULT_LISTEN
            ask_value "监听地址" "$listen"
            listen=$ASK_RESULT
            validate_listen "$listen" || return 1
            ;;
        agent|server)
            hub=${DSH_REMOTE_HUB:-}
            [ -n "$hub" ] || hub=$(conf_get "$conf" HUB)
            [ -n "$hub" ] || hub=$DEFAULT_HUB
            if [ -z "$hub" ] && [ "${DSH_REMOTE_ASSUME_YES:-0}" != 1 ]; then
                ask "中继地址（IP:端口）" ""
                hub=$ASK_RESULT
            fi
            validate_hub "$hub" || return 1
            ;;
    esac

    [ -n "$key" ] || key=$existing_key
    if [ -n "$existing_key" ] && [ -z "${DSH_REMOTE_KEY:-}" ]; then
        ask_secret "房间 Key" "$key" "回车保留现有"
    elif [ -n "$key" ]; then
        ask_secret "房间 Key" "$key" "回车使用此 Key"
    else
        ask_secret "房间 Key" "" "留空自动生成"
    fi
    key=$ASK_RESULT
    if [ -z "$key" ] && [ -x "$BIN" ]; then
        key=$("$BIN" genkey 2>/dev/null || true)
        [ -n "$key" ] && generated=1
    fi
    validate_key "$key" || return 1

    name=${DSH_REMOTE_NAME:-}
    [ -n "$name" ] || name=$(conf_get "$conf" NAME)
    [ -n "$name" ] || name=$DEFAULT_NAME
    if [ "$role" = server ] && [ -z "${DSH_REMOTE_NAME:-}" ] && [ -z "$(conf_get "$conf" NAME)" ]; then
        name=dsh
    fi
    ask_value "目标名" "$name"
    name=$ASK_RESULT
    case "$name" in
        '')
            error "目标名不能为空"
            return 1
            ;;
        *[[:space:]]*)
            error "目标名不能包含空白字符"
            return 1
            ;;
    esac

    if [ "$role" != server ]; then
        roots=${DSH_REMOTE_ROOTS:-}
        [ -n "$roots" ] || roots=$(conf_get "$conf" ROOTS)
        [ -n "$roots" ] || roots=$DEFAULT_ROOTS
        ask_value "允许访问的目录（冒号分隔）" "$roots"
        roots=$ASK_RESULT
        validate_roots "$roots" || return 1

        allow_bash=${DSH_REMOTE_ALLOW_BASH:-}
        [ -n "$allow_bash" ] || allow_bash=$(conf_get "$conf" ALLOW_BASH)
        [ -n "$allow_bash" ] || allow_bash=1
        ask_value "允许 bash 工具（1/0）" "$allow_bash"
        allow_bash=$ASK_RESULT
        validate_allow_bash "$allow_bash" || return 1

        max_output=${DSH_REMOTE_MAX_OUTPUT:-}
        [ -n "$max_output" ] || max_output=$(conf_get "$conf" MAX_OUTPUT)
        [ -n "$max_output" ] || max_output=$DEFAULT_MAX_OUTPUT
        ask_value "单次输出上限（字节）" "$max_output"
        max_output=$ASK_RESULT
        validate_num "MAX_OUTPUT" "$max_output" || return 1

        max_file=${DSH_REMOTE_MAX_FILE:-}
        [ -n "$max_file" ] || max_file=$(conf_get "$conf" MAX_FILE)
        [ -n "$max_file" ] || max_file=$DEFAULT_MAX_FILE
        ask_value "单文件传输上限（字节）" "$max_file"
        max_file=$ASK_RESULT
        validate_num "MAX_FILE" "$max_file" || return 1
    fi

    mkdir -p "$CONF_DIR" || {
        error "无法创建配置目录: $CONF_DIR"
        return 1
    }
    chmod 700 "$CONF_DIR" 2>/dev/null || true
    tmp="$conf.tmp.$$"
    {
        printf '# dsh-remote %s 配置（由 dsh-remote.sh 生成）\n' "$role"
        printf '# 权限 600；KEY 是三端互信的唯一凭据，请勿泄露。\n'
        case "$role" in
            hub)
                printf 'LISTEN=%s\n' "$listen"
                printf 'KEY=%s\n' "$key"
                printf 'NAME=%s\n' "$name"
                printf 'ROOTS=%s\n' "$roots"
                printf 'ALLOW_BASH=%s\n' "$allow_bash"
                printf 'MAX_OUTPUT=%s\n' "$max_output"
                printf 'MAX_FILE=%s\n' "$max_file"
                printf 'LOG=stderr\n'
                ;;
            agent)
                printf 'HUB=%s\n' "$hub"
                printf 'KEY=%s\n' "$key"
                printf 'NAME=%s\n' "$name"
                printf 'ROOTS=%s\n' "$roots"
                printf 'ALLOW_BASH=%s\n' "$allow_bash"
                printf 'MAX_OUTPUT=%s\n' "$max_output"
                printf 'MAX_FILE=%s\n' "$max_file"
                printf 'LOG=stderr\n'
                ;;
            server)
                printf 'HUB=%s\n' "$hub"
                printf 'KEY=%s\n' "$key"
                printf 'NAME=%s\n' "$name"
                gh_repo=${DSH_REMOTE_GH_REPO:-}
                [ -n "$gh_repo" ] || gh_repo=$(conf_get "$conf" GH_REPO)
                if [ -n "$gh_repo" ]; then
                    printf 'GH_REPO=%s\n' "$gh_repo"
                fi
                printf 'LOG=stderr\n'
                ;;
        esac
    } >"$tmp" || {
        rm -f "$tmp"
        error "写入临时配置失败: $tmp"
        return 1
    }
    if ! chmod 600 "$tmp" || ! mv -f "$tmp" "$conf"; then
        rm -f "$tmp"
        error "写入配置失败: $conf"
        return 1
    fi
    info "已写入配置: $conf（600）"
    if [ "$generated" = 1 ]; then
        info "已生成房间 Key: $key"
        warn "请把同一个 Key 填到其他两端；Key 只会写入 600 权限的配置文件"
    fi
}

write_unit() {
    role=$1
    conf=$2
    unit=$(unit_path "$role")
    mkdir -p "$SYSTEMD_DIR" || {
        error "无法创建 systemd 目录: $SYSTEMD_DIR"
        return 1
    }
    private_tmp=true
    roots=$(conf_get "$conf" ROOTS)
    if [ -n "$roots" ]; then
        old_ifs=$IFS
        IFS=:
        for root in $roots; do
            case "$root" in
                /|/tmp|/tmp/*) private_tmp=false ;;
            esac
        done
        IFS=$old_ifs
    fi
    tmp="$unit.tmp.$$"
    cat >"$tmp" <<EOF
[Unit]
Description=dsh-remote $role
Documentation=https://github.com/$REPO
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$BIN $role --config $conf
Restart=on-failure
RestartSec=2
LimitNOFILE=65535
NoNewPrivileges=true
PrivateTmp=$private_tmp

[Install]
WantedBy=multi-user.target
EOF
    if ! chmod 644 "$tmp" || ! mv -f "$tmp" "$unit"; then
        rm -f "$tmp"
        error "写入 systemd unit 失败: $unit"
        return 1
    fi
    info "已写入 systemd unit: $unit"
}

pid_matches() {
    pid=$1
    conf=$2
    [ -n "$pid" ] || return 1
    case "$pid" in
        *[!0-9]*) return 1 ;;
    esac
    [ -d "/proc/$pid" ] || return 1
    state=$(awk '/^State:/{print $2}' "/proc/$pid/status" 2>/dev/null || true)
    [ "$state" = Z ] && return 1
    exe=$(readlink "/proc/$pid/exe" 2>/dev/null || true)
    case "$exe" in
        *" (deleted)") exe=${exe% (deleted)} ;;
    esac
    [ "$exe" = "$BIN" ] || return 1
    [ -r "/proc/$pid/cmdline" ] || return 1
    tr '\000' '\n' <"/proc/$pid/cmdline" 2>/dev/null | awk -v conf="$conf" '
        previous == "--config" {
            if ($0 == conf) {
                found = 1
            }
        }
        {
            previous = $0
        }
        END {
            exit found ? 0 : 1
        }
    '
}

find_managed_pid() {
    conf=$1
    exclude=${2:-}
    for link in /proc/[0-9]*/exe; do
        [ -L "$link" ] || continue
        pid=${link#/proc/}
        pid=${pid%/exe}
        [ "$pid" = "$$" ] && continue
        [ -n "$exclude" ] && [ "$pid" = "$exclude" ] && continue
        if pid_matches "$pid" "$conf"; then
            printf '%s\n' "$pid"
            return 0
        fi
    done
    return 1
}

write_pid_file() {
    role=$1
    pid=$2
    mkdir -p "$STATE_DIR" || return 1
    file=$(pid_path "$role")
    tmp="$file.tmp.$$"
    if printf '%s\n' "$pid" >"$tmp" && chmod 644 "$tmp" && mv -f "$tmp" "$file"; then
        return 0
    fi
    rm -f "$tmp"
    return 1
}

alive_pid() {
    role=$1
    conf=$2
    file=$(pid_path "$role")
    if [ -r "$file" ]; then
        read -r pid <"$file" || pid=
        if pid_matches "$pid" "$conf"; then
            printf '%s\n' "$pid"
            return 0
        fi
        rm -f "$file"
    fi
    pid=$(find_managed_pid "$conf" "" 2>/dev/null || true)
    [ -n "$pid" ] || return 0
    write_pid_file "$role" "$pid" 2>/dev/null || true
    printf '%s\n' "$pid"
}

start_pid() {
    role=$1
    conf=$2
    mkdir -p "$STATE_DIR" "$LOG_DIR" || {
        error "无法创建 $STATE_DIR 或 $LOG_DIR"
        return 1
    }
    pid=$(alive_pid "$role" "$conf")
    if [ -n "$pid" ]; then
        warn "$role 已在运行 (PID: $pid)"
        return 0
    fi
    log="$LOG_DIR/$role.log"
    if command -v setsid >/dev/null 2>&1; then
        setsid "$BIN" "$role" --config "$conf" </dev/null >>"$log" 2>&1 &
    else
        "$BIN" "$role" --config "$conf" </dev/null >>"$log" 2>&1 &
    fi
    launch=$!
    pid=
    tries=0
    while [ "$tries" -lt 5 ]; do
        if pid_matches "$launch" "$conf"; then
            pid=$launch
        else
            pid=$(find_managed_pid "$conf" "$launch" 2>/dev/null || true)
        fi
        [ -n "$pid" ] && break
        tries=$((tries + 1))
        [ "$tries" -lt 5 ] && sleep 1
    done
    if [ -n "$pid" ]; then
        settle=0
        while [ "$settle" -lt 2 ]; do
            sleep 1
            if ! pid_matches "$pid" "$conf"; then
                pid=
                break
            fi
            settle=$((settle + 1))
        done
    fi
    if [ -z "$pid" ]; then
        kill -15 "$launch" 2>/dev/null || true
        rm -f "$(pid_path "$role")"
        error "$role 启动后未能保持运行，最近日志:"
        [ -f "$log" ] && tail -n 20 "$log" >&2
        return 1
    fi
    if ! write_pid_file "$role" "$pid"; then
        kill -15 "$pid" 2>/dev/null || true
        error "无法写入 PID 文件"
        return 1
    fi
    info "$role 已启动 (PID: $pid，日志: $log)"
}

stop_pid() {
    role=$1
    conf=$2
    pid=$(alive_pid "$role" "$conf")
    if [ -z "$pid" ]; then
        rm -f "$(pid_path "$role")"
        info "$role 当前未运行"
        return 0
    fi
    info "发送 SIGTERM 给 $role (PID: $pid)..."
    kill -15 "$pid" 2>/dev/null || true
    waited=0
    while pid_matches "$pid" "$conf" && [ "$waited" -lt 10 ]; do
        sleep 1
        waited=$((waited + 1))
    done
    if pid_matches "$pid" "$conf"; then
        warn "进程未退出，发送 SIGKILL..."
        kill -9 "$pid" 2>/dev/null || true
        sleep 1
    fi
    rm -f "$(pid_path "$role")"
    info "$role 已停止"
}

start_role() {
    role=$1
    if [ "$role" = server ]; then
        info "server 角色由 DSH 以 stdio 拉起，不单独启动；请在 DSH 配置中使用 server --config"
        return 0
    fi
    conf=$(conf_path "$role")
    [ -f "$conf" ] || {
        error "找不到配置: $conf（先运行 install）"
        return 1
    }
    [ -x "$BIN" ] || {
        error "找不到可执行文件: $BIN（先运行 install）"
        return 1
    }
    if role_managed "$role"; then
        service=$(service_name "$role")
        systemctl start "$service" || {
            error "systemctl start 失败: journalctl -u $service -n 30"
            return 1
        }
        sleep 1
        if systemctl is-active --quiet "$service"; then
            info "$role 已启动（systemd: $service）"
        else
            error "启动后服务未保持运行: journalctl -u $service -n 30"
            return 1
        fi
    else
        start_pid "$role" "$conf"
    fi
}

stop_role() {
    role=$1
    if [ "$role" = server ]; then
        info "server 进程由 DSH 管理，请重启 dsh web 使其退出"
        return 0
    fi
    conf=$(conf_path "$role")
    if role_managed "$role"; then
        service=$(service_name "$role")
        if systemctl is-active --quiet "$service"; then
            systemctl stop "$service" || {
                error "systemctl stop 失败: $service"
                return 1
            }
            info "$role 已停止（systemd: $service）"
        else
            info "$role 当前未运行（systemd: $service）"
        fi
        rm -f "$(pid_path "$role")"
        return 0
    fi
    stop_pid "$role" "$conf"
}

restart_role() {
    role=$1
    if [ "$role" = server ]; then
        info "server 进程由 DSH 管理，请重启 dsh web 使新配置或新二进制生效"
        return 0
    fi
    conf=$(conf_path "$role")
    if role_managed "$role"; then
        service=$(service_name "$role")
        systemctl restart "$service" || {
            error "systemctl restart 失败: journalctl -u $service -n 30"
            return 1
        }
        sleep 1
        if systemctl is-active --quiet "$service"; then
            info "$role 已重启（systemd: $service）"
        else
            error "重启后服务未保持运行: journalctl -u $service -n 30"
            return 1
        fi
    else
        stop_pid "$role" "$conf" || return 1
        start_pid "$role" "$conf" || return 1
    fi
}

status_role() {
    role=$1
    conf=$(conf_path "$role")
    printf '%s\n' "----------------------------------------"
    printf '角色: %s\n' "$role"
    printf '二进制: %s\n' "$BIN"
    printf '配置: %s\n' "$conf"
    version=$(bin_version)
    if [ -n "$version" ]; then
        printf '版本: %s\n' "$version"
    else
        printf '版本: 未安装或不可执行\n'
    fi
    printf '渠道: %s\n' "$(channel_state)"
    if [ ! -f "$conf" ]; then
        warn "配置不存在，请先运行: $0 $role install"
        return 0
    fi
    if [ "$role" = server ]; then
        printf '进程: 由 DSH 以 stdio 拉起，脚本不常驻守护\n'
        return 0
    fi
    if role_managed "$role"; then
        service=$(service_name "$role")
        systemctl status "$service" --no-pager -l || true
        return 0
    fi
    pid=$(alive_pid "$role" "$conf")
    if [ -n "$pid" ]; then
        info "运行中 (PID: $pid)"
        memory=$(awk '/^VmRSS:/{print $2, $3}' "/proc/$pid/status" 2>/dev/null || true)
        [ -n "$memory" ] && printf '内存: %s\n' "$memory"
        if command -v ps >/dev/null 2>&1; then
            ps -o lstart=,etime= -p "$pid" 2>/dev/null | sed 's/^/进程: /' || true
        fi
    else
        warn "$role 当前未运行"
    fi
    log="$LOG_DIR/$role.log"
    if [ -f "$log" ]; then
        printf '%s\n' "最近日志（$log）:"
        tail -n 15 "$log"
    fi
}

logs_role() {
    role=$1
    shift
    follow=0
    lines=100
    for arg in "$@"; do
        case "$arg" in
            -f|--follow) follow=1 ;;
            ''|*[!0-9]*)
                error "logs 参数只能是 -f 或行数: $arg"
                return 2
                ;;
            *) lines=$arg ;;
        esac
    done
    conf=$(conf_path "$role")
    configured_log=$(conf_get "$conf" LOG)
    case "$configured_log" in
        ''|stderr) configured_log= ;;
    esac
    if [ -n "$configured_log" ]; then
        if [ -f "$configured_log" ]; then
            if [ "$follow" = 1 ]; then
                tail -n "$lines" -f "$configured_log"
            else
                tail -n "$lines" "$configured_log"
            fi
            return 0
        fi
        if [ "$role" = server ]; then
            warn "配置的日志文件不存在: $configured_log"
            return 1
        fi
        warn "配置的日志文件不存在: $configured_log，回退到默认日志"
    fi
    if [ "$role" = server ]; then
        info "server 的日志在 DSH 侧（journalctl -u dsh-web 或 dsh web 的输出）"
        info "如需独立日志，可在 $conf 中设置 LOG=/var/log/dsh-remote/server.log"
        return 0
    fi
    if role_managed "$role"; then
        if ! command -v journalctl >/dev/null 2>&1; then
            error "缺少 journalctl"
            return 1
        fi
        service=$(service_name "$role")
        if [ "$follow" = 1 ]; then
            journalctl -u "$service" -n "$lines" -f
        else
            journalctl -u "$service" -n "$lines" --no-pager
        fi
        return 0
    fi
    log="$LOG_DIR/$role.log"
    if [ ! -f "$log" ]; then
        warn "日志文件不存在: $log（$role 可能从未用脚本启动）"
        return 1
    fi
    if [ "$follow" = 1 ]; then
        tail -n "$lines" -f "$log"
    else
        tail -n "$lines" "$log"
    fi
}

doctor_role() {
    role=$1
    conf=$(conf_path "$role")
    failed=0
    printf '%s\n' "----------------------------------------"
    info "检查 $role"
    info "发布渠道: $(channel_state)"
    if [ -x "$BIN" ]; then
        version=$(bin_version)
        info "二进制可执行: $BIN（${version:-未知版本}）"
    else
        error "二进制不存在或不可执行: $BIN"
        failed=1
    fi
    if [ ! -r "$conf" ]; then
        error "配置不存在或不可读: $conf"
        return 1
    fi
    info "配置可读: $conf"
    mode=
    if stat -c %a "$conf" >/dev/null 2>&1; then
        mode=$(stat -c %a "$conf" 2>/dev/null)
    elif stat -f %Lp "$conf" >/dev/null 2>&1; then
        mode=$(stat -f %Lp "$conf" 2>/dev/null)
    fi
    case "$mode" in
        ''|600) : ;;
        *) warn "配置权限建议 600，当前为: $mode" ;;
    esac
    key=$(conf_get "$conf" KEY)
    if validate_key "$key" 2>/dev/null; then
        info "Key 格式检查通过"
    else
        error "Key 缺失或格式不正确"
        failed=1
    fi
    case "$role" in
        hub)
            listen=$(conf_get "$conf" LISTEN)
            if [ -z "$listen" ]; then
                error "缺少 LISTEN"
                failed=1
            else
                info "监听地址: $listen"
                port=${listen##*:}
                if command -v ss >/dev/null 2>&1; then
                    if ss -ltn 2>/dev/null | awk '{print $4}' | grep -q "[:.]$port\$"; then
                        info "端口 $port 正在监听"
                    else
                        warn "端口 $port 未监听（服务可能未启动）"
                    fi
                fi
            fi
            ;;
        agent|server)
            hub=$(conf_get "$conf" HUB)
            if [ -z "$hub" ]; then
                error "缺少 HUB"
                failed=1
            else
                info "中继地址: $hub"
            fi
            ;;
    esac
    roots=$(conf_get "$conf" ROOTS)
    if [ -n "$roots" ]; then
        old_ifs=$IFS
        IFS=:
        for root in $roots; do
            if [ -d "$root" ]; then
                info "ROOTS 存在: $root"
            else
                warn "ROOTS 不存在: $root"
            fi
        done
        IFS=$old_ifs
    fi
    if [ "$role" != server ] && [ "$role" != hub ] && [ "$failed" = 0 ]; then
        info "测试到中继的连通性..."
        if command -v timeout >/dev/null 2>&1; then
            output=$(timeout 20 "$BIN" targets --config "$conf" 2>&1)
            result=$?
        else
            output=$("$BIN" targets --config "$conf" 2>&1)
            result=$?
        fi
        if [ "$result" -eq 0 ]; then
            info "中继连接正常，在线目标:"
            printf '%s\n' "$output"
        else
            error "连接中继或列出目标失败（退出码 $result）:"
            printf '%s\n' "$output" >&2
            failed=1
        fi
    fi
    if [ "$failed" = 0 ]; then
        info "$role 自检通过"
    else
        error "$role 自检发现问题"
    fi
    return "$failed"
}

update_role() {
    role=$1
    conf=$(conf_path "$role")
    check_base || return 1
    if ! command -v curl >/dev/null 2>&1; then
        error "缺少 curl，无法下载 Release"
        return 1
    fi
    channel_before=$(channel_saved)
    channel_choose || return 1
    channel_persist "$CHANNEL" || warn "无法记录发布渠道到 $CHANNEL_FILE"
    if [ -n "$channel_before" ] && [ "$channel_before" != "$CHANNEL" ]; then
        info "发布渠道已切换: $(channel_label "$channel_before") → $(channel_label "$CHANNEL")"
    fi
    old_version=$(bin_version)
    if [ -n "$old_version" ]; then
        info "当前版本: $old_version"
    else
        info "当前未安装 $PROGRAM，直接安装$(channel_label "$CHANNEL")"
    fi
    was_systemd=0
    was_pid=
    server_pid=
    if role_managed "$role"; then
        if systemctl is-active --quiet "$(service_name "$role")"; then
            was_systemd=1
        fi
    elif [ "$role" = server ]; then
        [ -f "$conf" ] && server_pid=$(find_managed_pid "$conf" "" 2>/dev/null || true)
    else
        [ -f "$conf" ] && was_pid=$(alive_pid "$role" "$conf")
    fi
    fetch_release || return 1
    if [ "${DSH_REMOTE_FORCE:-0}" != 1 ] && [ "$CHANNEL" = stable ] && [ -n "$old_version" ] \
        && [ "$old_version" = "$FETCH_VERSION" ]; then
        info "已是最新版本 $FETCH_VERSION（DSH_REMOTE_FORCE=1 可强制重装）"
        return 0
    fi
    install_candidate "$FETCH_BIN" || return 1
    if [ -n "$old_version" ]; then
        info "已更新: $old_version → $FETCH_VERSION"
    else
        info "已安装 $PROGRAM $FETCH_VERSION"
    fi
    if [ "$role" = server ]; then
        if [ -n "$server_pid" ]; then
            warn "检测到 DSH 的 server 进程 (PID: $server_pid) 仍在使用旧二进制"
            warn "重启 dsh web 后新版本生效"
        fi
        return 0
    fi
    if [ "$was_systemd" = 1 ]; then
        systemctl daemon-reload 2>/dev/null || true
        if systemctl restart "$(service_name "$role")"; then
            info "$role 已按新版本重启（systemd）"
        else
            error "新版本已安装，但 systemctl restart 失败"
            return 1
        fi
    elif [ -n "$was_pid" ]; then
        stop_pid "$role" "$conf" || return 1
        start_pid "$role" "$conf" || return 1
    else
        info "$role 当前未运行，未自动启动"
    fi
}

install_role() {
    role=$1
    check_base || return 1
    conf=$(conf_path "$role")
    channel_reported=0
    if [ ! -x "$BIN" ]; then
        info "未找到 $BIN，从 Release 安装"
        channel_choose || return 1
        channel_persist "$CHANNEL" || warn "无法记录发布渠道到 $CHANNEL_FILE"
        fetch_release || return 1
        install_candidate "$FETCH_BIN" || return 1
        info "已安装 $PROGRAM $FETCH_VERSION 到 $BIN"
        channel_reported=1
    fi
    version=$(bin_version)
    if [ -z "$version" ]; then
        error "$BIN 无法运行（架构不匹配或文件损坏）"
        return 1
    fi
    info "使用二进制: $BIN（$PROGRAM $version）"
    if [ "$channel_reported" = 0 ]; then
        info "发布渠道: $(channel_state)"
    fi
    config_changed=0
    if [ -f "$conf" ]; then
        info "配置文件已存在: $conf"
        if confirm "是否重新填写配置" n; then
            write_config "$role" || return 1
            config_changed=1
        fi
    else
        write_config "$role" || return 1
        config_changed=1
    fi
    if [ "$role" = server ]; then
        info "server 不需要常驻服务，进程由 DSH 以 stdio 拉起"
        return 0
    fi
    if systemd_available; then
        write_unit "$role" "$conf" || return 1
        systemctl daemon-reload || {
            error "systemctl daemon-reload 失败"
            return 1
        }
        service=$(service_name "$role")
        if ! systemctl enable "$service"; then
            error "systemctl enable 失败: $service"
            return 1
        fi
        if systemctl is-active --quiet "$service"; then
            if [ "$config_changed" = 1 ]; then
                systemctl restart "$service" || {
                    error "配置已更新，但 systemctl restart 失败: journalctl -u $service -n 30"
                    return 1
                }
            fi
        else
            systemctl start "$service" || {
                error "systemctl start 失败: journalctl -u $service -n 30"
                return 1
            }
        fi
        sleep 1
        if systemctl is-active --quiet "$service"; then
            info "$role 已安装并启动（systemd: $service）"
        else
            error "服务未能保持运行: journalctl -u $service -n 30"
            return 1
        fi
    else
        if [ "$config_changed" = 1 ]; then
            pid=$(alive_pid "$role" "$conf")
            if [ -n "$pid" ]; then
                stop_pid "$role" "$conf" || return 1
            fi
        fi
        start_pid "$role" "$conf" || return 1
        info "$role 已安装并启动（PID 文件: $(pid_path "$role")）"
    fi
}

uninstall_role() {
    role=$1
    conf=$(conf_path "$role")
    if ! confirm "确认卸载 dsh-remote-$role（停止服务并删除 unit）" n; then
        info "已取消"
        return 0
    fi
    if role_managed "$role"; then
        service=$(service_name "$role")
        systemctl disable --now "$service" 2>/dev/null || warn "systemctl disable --now 失败: $service"
        rm -f "$(unit_path "$role")"
        systemctl daemon-reload 2>/dev/null || true
        systemctl reset-failed "$service" >/dev/null 2>&1 || true
        info "已删除 systemd unit: $(unit_path "$role")"
    elif [ "$role" != server ]; then
        stop_pid "$role" "$conf" || return 1
    fi
    if [ -f "$(unit_path "$role")" ]; then
        rm -f "$(unit_path "$role")"
        info "已删除残留 systemd unit: $(unit_path "$role")"
    fi
    rm -f "$(pid_path "$role")"
    if [ "$role" = server ]; then
        pid=$(find_managed_pid "$conf" "" 2>/dev/null || true)
        if [ -n "$pid" ]; then
            warn "检测到 DSH 的 server 进程仍在运行 (PID: $pid)，请重启 dsh web 使其退出"
        fi
    fi
    other_confs=0
    for file in "$CONF_DIR"/*.conf; do
        [ -f "$file" ] || continue
        [ "$file" = "$conf" ] && continue
        other_confs=1
    done
    if [ -f "$BIN" ]; then
        default_delete=y
        if [ "$other_confs" = 1 ]; then
            default_delete=n
            warn "还有其他角色的配置，删除二进制会影响它们"
        fi
        if confirm "删除二进制 $BIN" "$default_delete"; then
            rm -f "$BIN"
            info "已删除: $BIN"
        else
            info "已保留二进制: $BIN"
        fi
    fi
    if [ -f "$conf" ]; then
        info "配置与 Key 保留在: $conf"
        printf '如确认删除配置与 Key，请输入 yes（其他内容取消）: '
        answer=
        IFS= read -r answer || answer=
        if [ "$answer" = yes ]; then
            rm -f "$conf"
            info "已删除配置与 Key: $conf"
        else
            info "已保留配置与 Key"
        fi
    fi
}

role_state() {
    role=$1
    conf=$(conf_path "$role")
    if [ ! -f "$conf" ]; then
        printf '未安装'
        return 0
    fi
    if [ "$role" = server ]; then
        printf '已配置（由 DSH 拉起）'
        return 0
    fi
    if role_managed "$role"; then
        if systemctl is-active --quiet "$(service_name "$role")"; then
            printf '运行中（systemd）'
        else
            printf '已停止（systemd）'
        fi
        return 0
    fi
    pid=$(alive_pid "$role" "$conf")
    if [ -n "$pid" ]; then
        printf '运行中（PID %s）' "$pid"
    else
        printf '已停止'
    fi
}

role_menu() {
    role=$1
    while :; do
        [ -t 1 ] && printf '\033c'
        printf '%s\n' "========================================"
        printf ' dsh-remote %s\n' "$role"
        printf '%s\n' "========================================"
        printf ' 状态: %s\n' "$(role_state "$role")"
        printf ' 配置: %s\n' "$(conf_path "$role")"
        printf ' 渠道: %s\n' "$(channel_state)"
        printf '%s\n' "----------------------------------------"
        printf '%s\n' \
            '  1. install    安装/写配置' \
            '  2. start      启动' \
            '  3. stop       停止' \
            '  4. restart    重启' \
            '  5. status     查看状态' \
            '  6. update     更新二进制' \
            '  7. logs       查看日志' \
            '  8. doctor     自检' \
            '  9. uninstall  卸载' \
            '  0. 返回'
        printf '请选择 [0-9]: '
        choice=
        IFS= read -r choice || return 0
        printf '\n'
        case "$choice" in
            1) run_action "$role" install ;;
            2) run_action "$role" start ;;
            3) run_action "$role" stop ;;
            4) run_action "$role" restart ;;
            5) run_action "$role" status ;;
            6) run_action "$role" update ;;
            7) run_action "$role" logs ;;
            8) run_action "$role" doctor ;;
            9) run_action "$role" uninstall ;;
            0) return 0 ;;
            *) error "无效选项" ;;
        esac
        printf '\n按回车返回...'
        IFS= read -r _ || true
    done
}

apply_channel() {
    want=$(normalize_channel "$1") || {
        error "未知发布渠道: $1（可选 stable 正式版 / dev 预发布版）"
        return 2
    }
    if ! channel_persist "$want"; then
        error "切换渠道失败；需要 root 权限，请用 sudo 运行脚本"
        return 1
    fi
    CHANNEL=$want
    info "发布渠道已切换为: $(channel_label "$want")"
    info "渠道文件: $CHANNEL_FILE"
    info "对 hub/agent 执行 update（或重新 install）后生效"
}

channel_cli() {
    check_base || return 1
    if [ "$#" -eq 0 ]; then
        if resolve_channel; then
            printf '当前发布渠道: %s（%s）\n' "$(channel_label "$CHANNEL")" "$CHANNEL"
            printf '渠道文件: %s\n' "$CHANNEL_FILE"
            printf '用法: %s channel stable|dev\n' "$0"
            return 0
        fi
        return 2
    fi
    require_root channel "$@" || return 1
    apply_channel "$1"
}

channel_menu() {
    while :; do
        [ -t 1 ] && printf '\033c'
        printf '%s\n' "========================================"
        printf ' dsh-remote 发布渠道\n'
        printf '%s\n' "========================================"
        printf ' 当前: %s\n' "$(channel_state)"
        printf ' 文件: %s\n' "$CHANNEL_FILE"
        printf '%s\n' "----------------------------------------" \
            '  1. 正式版 stable（Releases latest）' \
            "  2. 预发布版 dev（$(dev_label)）" \
            '  0. 返回'
        printf '请选择 [0-2]: '
        choice=
        IFS= read -r choice || return 0
        printf '\n'
        case "$choice" in
            1) apply_channel stable ;;
            2) apply_channel dev ;;
            0) return 0 ;;
            *) error "无效选项" ;;
        esac
        printf '\n按回车返回...'
        IFS= read -r _ || true
    done
}

menu() {
    while :; do
        [ -t 1 ] && printf '\033c'
        printf '%s\n' "========================================"
        printf ' dsh-remote 管理脚本\n'
        printf '%s\n' "========================================"
        printf '  hub    : %s\n' "$(role_state hub)"
        printf '  agent  : %s\n' "$(role_state agent)"
        printf '  server : %s\n' "$(role_state server)"
        printf '  渠道   : %s\n' "$(channel_state)"
        printf '%s\n' "----------------------------------------"
        printf '%s\n' \
            '  1. 管理中继 hub' \
            '  2. 管理被控端 agent' \
            '  3. 管理服务端 server' \
            '  4. 切换发布渠道' \
            '  0. 退出'
        printf '请选择 [0-4]: '
        choice=
        IFS= read -r choice || return 0
        case "$choice" in
            1) role_menu hub ;;
            2) role_menu agent ;;
            3) role_menu server ;;
            4) channel_menu ;;
            0) return 0 ;;
            *) error "无效选项" ;;
        esac
    done
}

run_action() {
    role=$1
    action=$2
    shift 2
    check_base || return 1
    case "$action" in
        install|update|uninstall)
            require_root "$role" "$action" "$@" || return 1
            ;;
        start|stop|restart)
            if [ "$role" != server ]; then
                require_root "$role" "$action" "$@" || return 1
            fi
            ;;
    esac
    case "$role" in
        hub|agent|server) : ;;
        *)
            error "未知角色: $role"
            return 2
            ;;
    esac
    case "$action" in
        install) install_role "$role" ;;
        start) start_role "$role" ;;
        stop) stop_role "$role" ;;
        restart) restart_role "$role" ;;
        status) status_role "$role" ;;
        update) update_role "$role" ;;
        logs) logs_role "$role" "$@" ;;
        doctor) doctor_role "$role" ;;
        uninstall) uninstall_role "$role" ;;
        *)
            error "未知动作: $action"
            usage
            return 2
            ;;
    esac
}

main() {
    if [ "$#" -eq 0 ]; then
        menu
        return $?
    fi
    case "$1" in
        -h|--help|help)
            usage
            return 0
            ;;
        -V|--version)
            if [ -x "$BIN" ]; then
                "$BIN" --version
            else
                printf '%s\n' "$PROGRAM（未安装到 $BIN）"
            fi
            return 0
            ;;
        channel|channels)
            shift
            channel_cli "$@"
            return $?
            ;;
    esac
    role=$1
    shift
    case "$role" in
        hub|agent|server) : ;;
        *)
            error "未知角色: $role"
            usage
            return 2
            ;;
    esac
    if [ "$#" -eq 0 ]; then
        error "缺少动作"
        usage
        return 2
    fi
    action=$1
    shift
    run_action "$role" "$action" "$@"
}

main "$@"
