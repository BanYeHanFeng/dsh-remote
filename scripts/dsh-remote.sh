#!/usr/bin/env bash
set -euo pipefail

BIN_PATH="/usr/local/bin/dsh-remote"
ETC_DIR="/etc/dsh-remote"
UNIT_DIR="/etc/systemd/system"
SOCKET_PATH="/run/dsh-remote/controller.sock"
DEFAULT_PORT="7443"
REPO="BanYeHanFeng/dsh-remote"
SCRIPT_URL="https://raw.githubusercontent.com/${REPO}/dev/scripts/dsh-remote.sh"

UI=""

ROLES=(controller "控制端" relay "中继" agent "被控端")

msg() { printf '%s\n' "$*" >&2; }
die() { printf '错误：%s\n' "$*" >&2; exit 1; }

need_root() {
  [[ ${EUID} -eq 0 ]] || die "需要 root 权限，请用 sudo 运行本脚本"
}

detect_ui() {
  if [[ -n ${DSH_REMOTE_UI:-} ]]; then
    UI="${DSH_REMOTE_UI}"
  elif command -v whiptail >/dev/null 2>&1 && [[ -t 1 ]]; then
    UI="whiptail"
  elif command -v dialog >/dev/null 2>&1 && [[ -t 1 ]]; then
    UI="dialog"
  else
    UI="plain"
  fi
}

ui_menu() {
  local title="$1" text="$2"
  shift 2
  local -a items=("$@")
  local choice
  if [[ ${UI} != "plain" ]]; then
    if [[ ${UI} == "whiptail" ]]; then
      choice=$(whiptail --title "${title}" --menu "${text}" 20 72 10 "${items[@]}" 3>&1 1>&2 2>&3) || return 0
    else
      choice=$(dialog --stdout --title "${title}" --menu "${text}" 20 72 10 "${items[@]}") || return 0
    fi
    printf '%s\n' "${choice}"
    return 0
  fi
  local -a keys=()
  local i=0 n
  printf '\n== %s ==\n%s\n' "${title}" "${text}" >&2
  while [[ ${i} -lt ${#items[@]} ]]; do
    keys+=("${items[i]}")
    printf '  %d) %s\n' "$((i / 2 + 1))" "${items[i + 1]:-}" >&2
    i=$((i + 2))
  done
  while true; do
    read -r -p "请输入编号（回车返回）：" choice || return 0
    [[ -n ${choice} ]] || return 0
    if [[ ${choice} =~ ^[0-9]{1,3}$ ]]; then
      n=$((10#${choice}))
      if ((n >= 1 && n <= ${#keys[@]})); then
        printf '%s\n' "${keys[n - 1]}"
        return 0
      fi
    fi
    msg "无效编号：${choice}"
  done
}

ui_input() {
  local title="$1" prompt="$2" default="${3:-}"
  local value
  if [[ ${UI} == "plain" ]]; then
    read -r -p "${prompt}${default:+ [${default}]}：" value
    printf '%s\n' "${value:-${default}}"
    return 0
  fi
  if [[ ${UI} == "whiptail" ]]; then
    value=$(whiptail --title "${title}" --inputbox "${prompt}" 12 72 "${default}" 3>&1 1>&2 2>&3)
  else
    value=$(dialog --stdout --title "${title}" --inputbox "${prompt}" 12 72 "${default}")
  fi
  printf '%s\n' "${value}"
}

ui_password() {
  local title="$1" prompt="$2"
  local value
  if [[ ${UI} == "plain" ]]; then
    read -r -s -p "${prompt}：" value
    printf '\n' >&2
    printf '%s\n' "${value}"
    return 0
  fi
  if [[ ${UI} == "whiptail" ]]; then
    value=$(whiptail --title "${title}" --passwordbox "${prompt}" 12 72 3>&1 1>&2 2>&3)
  else
    value=$(dialog --stdout --title "${title}" --passwordbox "${prompt}" 12 72)
  fi
  printf '%s\n' "${value}"
}

ui_yesno() {
  local title="$1" prompt="$2" default="${3:-n}"
  local -a extra=()
  [[ ${default} == "y" ]] || extra=(--defaultno)
  if [[ ${UI} == "plain" ]]; then
    local answer hint="[y/N]"
    [[ ${default} == "y" ]] && hint="[Y/n]"
    read -r -p "${prompt} ${hint}：" answer
    answer="${answer:-${default}}"
    [[ ${answer} == "y" || ${answer} == "Y" ]]
    return
  fi
  if [[ ${UI} == "whiptail" ]]; then
    whiptail --title "${title}" --yesno "${prompt}" 12 72 "${extra[@]}"
  else
    dialog --title "${title}" --yesno "${prompt}" 12 72 "${extra[@]}"
  fi
}

gen_psk() {
  head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'
}

normalize_addr() {
  local addr="$1"
  if [[ ${addr} =~ :[0-9]+$ ]]; then
    printf '%s\n' "${addr}"
  else
    printf '%s:%s\n' "${addr}" "${DEFAULT_PORT}"
  fi
}

toml_escape() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

toml_tags() {
  local literal="" tag
  for tag in "$@"; do
    [[ -z ${tag} ]] && continue
    [[ -n ${literal} ]] && literal+=", "
    literal+="\"$(toml_escape "${tag}")\""
  done
  printf '%s' "${literal}"
}

parse_tags() {
  TAG_LIST=()
  local item
  local -a raw=()
  IFS=',' read -ra raw <<<"$1"
  for item in "${raw[@]:-}"; do
    item=$(printf '%s' "${item}" | tr -d ' ')
    [[ -n ${item} ]] && TAG_LIST+=("${item}")
  done
}

asset_url() {
  local asset="$1" url tag
  url="https://github.com/${REPO}/releases/latest/download/${asset}"
  if curl -fsIL --max-time 15 "${url}" >/dev/null 2>&1; then
    printf '%s\n' "${url}"
    return 0
  fi
  tag=$(curl -fsSL --max-time 15 "https://api.github.com/repos/${REPO}/releases?per_page=1" 2>/dev/null |
    sed -n 's/^ *"tag_name": *"\([^"]*\)".*/\1/p' | head -n1)
  [[ -n ${tag} ]] || return 1
  printf 'https://github.com/%s/releases/download/%s/%s\n' "${REPO}" "${tag}" "${asset}"
}

install_binary() {
  local arch asset url tmp
  case $(uname -m) in
    x86_64 | amd64) arch="x86_64" ;;
    aarch64 | arm64) arch="aarch64" ;;
    *) die "不支持的架构：$(uname -m)" ;;
  esac
  command -v zstd >/dev/null 2>&1 || die "解包需要 zstd，请先安装（如 apt install zstd）"
  asset="dsh-remote-linux-${arch}.tar.zst"
  url=$(asset_url "${asset}") || die "找不到发布产物：${asset}，请先在仓库 Actions 中发布一次"
  msg "下载二进制：${url}"
  tmp=$(mktemp -d)
  if ! curl -fsSL --retry 2 "${url}" -o "${tmp}/${asset}"; then
    rm -rf "${tmp}"
    die "下载失败：${url}"
  fi
  if ! zstd -dcq "${tmp}/${asset}" | tar -x -C "${tmp}" dsh-remote; then
    rm -rf "${tmp}"
    die "解包失败：${url}"
  fi
  install -m 0755 "${tmp}/dsh-remote" "${BIN_PATH}.new"
  mv -f "${BIN_PATH}.new" "${BIN_PATH}"
  rm -rf "${tmp}"
  msg "二进制就绪：${BIN_PATH}"
}

prepare_etc() {
  mkdir -p "${ETC_DIR}"
  chmod 0700 "${ETC_DIR}"
}

write_controller_config() {
  local listen="$1" relay="$2" mcp_http="$3" psk="$4"
  prepare_etc
  local file="${ETC_DIR}/controller.toml"
  umask 077
  cat >"${file}" <<EOF
listen = "$(toml_escape "${listen}")"
relay = "$(toml_escape "${relay}")"
socket = "${SOCKET_PATH}"
mcp_http = "$(toml_escape "${mcp_http}")"
psk = "$(toml_escape "${psk}")"
EOF
  chmod 0600 "${file}"
  msg "已写入 ${file}"
}

write_relay_config() {
  local listen="$1" id="$2" psk="$3" local_enabled="$4"
  shift 4
  prepare_etc
  local file="${ETC_DIR}/relay.toml"
  umask 077
  cat >"${file}" <<EOF
listen = "$(toml_escape "${listen}")"
psk = "$(toml_escape "${psk}")"
EOF
  if [[ ${local_enabled} == "1" ]]; then
    cat >>"${file}" <<EOF
id = "$(toml_escape "${id}")"
tags = [$(toml_tags "$@")]
EOF
  else
    printf 'local = false\n' >>"${file}"
  fi
  chmod 0600 "${file}"
  msg "已写入 ${file}"
}

write_agent_config() {
  local connect="$1" id="$2" psk="$3"
  shift 3
  local tags=("$@")
  prepare_etc
  local file="${ETC_DIR}/agent.toml"
  local tags_literal
  tags_literal=$(toml_tags "${tags[@]:-}")
  umask 077
  cat >"${file}" <<EOF
connect = "$(toml_escape "${connect}")"
id = "$(toml_escape "${id}")"
tags = [${tags_literal}]
psk = "$(toml_escape "${psk}")"
EOF
  chmod 0600 "${file}"
  msg "已写入 ${file}"
}

unit_path() {
  printf '%s/dsh-remote-%s.service\n' "${UNIT_DIR}" "$1"
}

write_unit() {
  local role="$1"
  local unit
  unit=$(unit_path "${role}")
  local runtime=""
  if [[ ${role} == "controller" ]]; then
    runtime=$'RuntimeDirectory=dsh-remote\nRuntimeDirectoryMode=0700'
  fi
  cat >"${unit}" <<EOF
[Unit]
Description=dsh-remote ${role}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
${runtime}
ExecStart=${BIN_PATH} ${role} --config ${ETC_DIR}/${role}.toml
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
  chmod 0644 "${unit}"
  systemctl daemon-reload
  msg "已写入 ${unit}"
}

enable_service() {
  local role="$1"
  write_unit "${role}"
  systemctl enable "dsh-remote-${role}.service" >/dev/null 2>&1 || true
  systemctl restart "dsh-remote-${role}.service"
  sleep 1
  systemctl --no-pager --lines=0 status "dsh-remote-${role}.service" || true
}

service_action() {
  local role="$1" action="$2"
  case ${action} in
    status) systemctl --no-pager --lines=20 status "dsh-remote-${role}.service" || true ;;
    logs)
      trap '' INT
      (trap - INT; journalctl -u "dsh-remote-${role}.service" -n 100 --no-pager -f) || true
      trap - INT
      ;;
    uninstall)
      systemctl disable --now "dsh-remote-${role}.service" >/dev/null 2>&1 || true
      rm -f "$(unit_path "${role}")"
      systemctl daemon-reload
      msg "已停止并移除 dsh-remote-${role}.service（配置文件与二进制保留）"
      ;;
    *)
      systemctl "${action}" "dsh-remote-${role}.service"
      systemctl is-active "dsh-remote-${role}.service" >&2 || true
      ;;
  esac
}

print_access_command() {
  local role="$1" connect="$2"
  local psk
  psk=$(sed -n 's/^psk = "\(.*\)"$/\1/p' "${ETC_DIR}/${role}.toml" | head -n1)
  [[ -n ${psk} ]] || die "读取 ${ETC_DIR}/${role}.toml 中的 psk 失败"
  local url="${DSH_REMOTE_SCRIPT_URL:-${SCRIPT_URL}}"
  msg ""
  msg "在被控端机器上执行下面这条命令即可完成静默接入："
  msg ""
  {
    printf 'curl -fsSL %s | sudo bash -s -- agent \\\n' "${url}"
    printf '  --connect %s \\\n' "${connect}"
    printf '  --psk %s \\\n' "${psk}"
    printf '  --id "$(hostname)"\n'
  } >&2
  cat <<EOF

说明：
  1. 若能用 scp 传递脚本，也可把 dsh-remote.sh 拷到被控端后执行同样的参数；
  2. 接入只依赖 PSK：握手用 PSK 双向校验，之后整条 TCP 链路用会话密钥加密；
  3. 被控端会自动安装二进制、写入 ${ETC_DIR}/agent.toml 并注册启动 dsh-remote-agent.service。

EOF
}

ask_psk() {
  local generated
  generated=$(gen_psk)
  if ui_yesno "预共享密钥" "是否自动生成 32 字节随机 PSK？\n（选否可手动输入，需与控制端/中继一致）"; then
    msg "已生成 PSK：${generated}"
    printf '%s\n' "${generated}"
    return
  fi
  ui_password "预共享密钥" "请输入 PSK"
}

configure_controller() {
  local listen relay_addr mcp_http psk
  listen=$(ui_input "控制端监听地址" "监听地址（如 0.0.0.0:${DEFAULT_PORT}）" "0.0.0.0:${DEFAULT_PORT}")
  relay_addr=$(ui_input "接入中继" "可选：要接入的中继地址，留空表示纯直连模式" "")
  mcp_http=$(ui_input "MCP HTTP" "可选：MCP Streamable HTTP 监听地址，留空表示不开启" "")
  psk=$(ask_psk)
  install_binary
  local relay_normalized=""
  if [[ -n ${relay_addr} ]]; then
    relay_normalized=$(normalize_addr "${relay_addr}")
  fi
  listen=$(normalize_addr "${listen}")
  write_controller_config "${listen}" "${relay_normalized}" "${mcp_http}" "${psk}"
  enable_service controller
  print_access_command controller "${listen}"
}

configure_relay() {
  local listen psk id tags local_enabled
  listen=$(ui_input "中继监听地址" "监听地址（如 0.0.0.0:${DEFAULT_PORT}）" "0.0.0.0:${DEFAULT_PORT}")
  local_enabled=0
  id=""
  tags=""
  if ui_yesno "中继本机节点" "是否允许控制端在中继本机上执行命令？\n（开启后中继本机会以节点形式出现在节点列表）" y; then
    local_enabled=1
    id=$(ui_input "本机节点标识" "中继本机作为节点时使用的标识" "$(hostname)")
    if [[ ${UI} != "plain" ]]; then
      tags=$(ui_input "本机节点标签" "可选：标签，多个用逗号分隔（如 env=prod,role=relay）" "")
    fi
  fi
  psk=$(ask_psk)
  install_binary
  listen=$(normalize_addr "${listen}")
  parse_tags "${tags}"
  write_relay_config "${listen}" "${id}" "${psk}" "${local_enabled}" "${TAG_LIST[@]:-}"
  enable_service relay
  print_access_command relay "${listen}"
}

configure_agent() {
  local connect id psk tags
  connect="${AGENT_CONNECT:-}"
  [[ -n ${connect} ]] || connect=$(ui_input "接入地址" "控制端或中继地址（如 10.0.0.1:${DEFAULT_PORT}）" "")
  [[ -n ${connect} ]] || die "接入地址不能为空"
  id="${AGENT_ID:-$(hostname)}"
  psk="${AGENT_PSK:-}"
  [[ -n ${psk} ]] || psk=$(ui_password "预共享密钥" "请输入与控制端/中继一致的 PSK")
  tags="${AGENT_TAGS:-}"
  if [[ -z ${tags} ]] && [[ ${UI} != "plain" ]]; then
    tags=$(ui_input "节点标签" "可选：标签，多个用逗号分隔（如 env=prod,role=web）" "")
  fi
  install_binary
  parse_tags "${tags}"
  write_agent_config "$(normalize_addr "${connect}")" "${id}" "${psk}" "${TAG_LIST[@]:-}"
  enable_service agent
}

service_menu() {
  local role action
  role=$(ui_menu "服务管理" "选择角色" "${ROLES[@]}")
  [[ -n ${role} ]] || return 1
  action=$(ui_menu "服务管理 - ${role}" "选择操作" \
    status "查看状态" start "启动" stop "停止" restart "重启" logs "实时日志" uninstall "卸载服务")
  [[ -n ${action} ]] || return 1
  service_action "${role}" "${action}"
}

access_menu() {
  local role addr
  role=$(ui_menu "一键接入命令" "选择服务角色" "${ROLES[@]:0:4}")
  [[ -n ${role} ]] || return 1
  addr=$(sed -n 's/^listen = "\(.*\)"$/\1/p' "${ETC_DIR}/${role}.toml" | head -n1)
  [[ -n ${addr} ]] || die "读取 ${ETC_DIR}/${role}.toml 中的 listen 失败"
  print_access_command "${role}" "${addr}"
}

main_menu() {
  while true; do
    local choice
    choice=$(ui_menu "dsh-remote 一键配置" "请选择要执行的操作" \
      controller "配置并部署控制端（controller）" \
      relay "配置并部署中继（relay）" \
      agent "配置并部署被控端（agent）" \
      service "服务管理（状态/启停/日志/卸载）" \
      access "生成被控端一键接入命令" \
      quit "退出") || return 0
    case ${choice} in
      controller) configure_controller ;;
      relay) configure_relay ;;
      agent) configure_agent ;;
      service) service_menu || continue ;;
      access) access_menu || continue ;;
      quit | "") return 0 ;;
    esac
    printf '\n按回车返回主菜单...' >&2
    read -r _ || true
  done
}

usage() {
  cat <<EOF
用法：dsh-remote.sh [子命令] [选项]

子命令：
  menu                    交互式菜单（默认）
  controller              交互式配置并部署控制端
  relay                   交互式配置并部署中继
  agent                   配置并部署被控端（支持全参数非交互）
  install                 按当前架构下载并安装最新发布的二进制
  status|start|stop|restart|logs|uninstall <controller|relay|agent>
  access <controller|relay>   打印被控端一键接入命令

通用选项：
  --non-interactive       纯文本交互（不弹窗）

agent 专用选项：
  --connect <地址>        控制端或中继地址
  --psk <密钥>            预共享密钥
  --id <节点标识>         节点标识，默认主机名
  --tag <k=v>             节点标签，可重复

环境变量：
  DSH_REMOTE_SCRIPT_URL   本脚本的公网地址（用于生成接入命令）
EOF
}

ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --non-interactive)
      UI="plain"
      shift
      ;;
    --connect)
      AGENT_CONNECT="$2"
      shift 2
      ;;
    --connect=*)
      AGENT_CONNECT="${1#*=}"
      shift
      ;;
    --psk)
      AGENT_PSK="$2"
      shift 2
      ;;
    --psk=*)
      AGENT_PSK="${1#*=}"
      shift
      ;;
    --id)
      AGENT_ID="$2"
      shift 2
      ;;
    --id=*)
      AGENT_ID="${1#*=}"
      shift
      ;;
    --tag)
      AGENT_TAGS="${AGENT_TAGS:+${AGENT_TAGS},}$2"
      shift 2
      ;;
    --tag=*)
      AGENT_TAGS="${AGENT_TAGS:+${AGENT_TAGS},}${1#*=}"
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

AGENT_CONNECT="${AGENT_CONNECT:-}"
AGENT_PSK="${AGENT_PSK:-}"
AGENT_ID="${AGENT_ID:-}"
AGENT_TAGS="${AGENT_TAGS:-}"

need_root
detect_ui

subcmd="${ARGS[0]:-menu}"
case ${subcmd} in
  menu) main_menu ;;
  controller) configure_controller ;;
  relay) configure_relay ;;
  agent) configure_agent ;;
  install) install_binary ;;
  access)
    role="${ARGS[1]:-}"
    if [[ -z ${role} ]]; then
      access_menu
    else
      addr=$(sed -n 's/^listen = "\(.*\)"$/\1/p' "${ETC_DIR}/${role}.toml" | head -n1)
      [[ -n ${addr} ]] || die "读取 ${ETC_DIR}/${role}.toml 中的 listen 失败"
      print_access_command "${role}" "${addr}"
    fi
    ;;
  status | start | stop | restart | logs | uninstall)
    role="${ARGS[1]:-}"
    [[ -n ${role} ]] || die "请指定角色：controller / relay / agent"
    service_action "${role}" "${subcmd}"
    ;;
  -h | --help | help) usage ;;
  *) usage; die "未知子命令：${subcmd}" ;;
esac
