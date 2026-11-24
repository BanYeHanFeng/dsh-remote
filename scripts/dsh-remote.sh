#!/usr/bin/env bash
set -euo pipefail

BIN_PATH="/usr/local/bin/dsh-remote"
ETC_DIR="/etc/dsh-remote"
UNIT_DIR="/etc/systemd/system"
SOCKET_PATH="/run/dsh-remote/controller.sock"
DEFAULT_PORT="7443"
REPO="BanYeHanFeng/dsh-remote"
SCRIPT_URL="https://raw.githubusercontent.com/${REPO}/dev/scripts/dsh-remote.sh"

ROLES=(controller "控制端" relay "中继" agent "被控端")

msg() { printf '%s\n' "$*" >&2; }
die() { printf '错误：%s\n' "$*" >&2; exit 1; }

need_root() {
  [[ ${EUID} -eq 0 ]] || die "需要 root 权限，请用 sudo 运行本脚本"
}

need_tty() {
  [[ -t 0 ]] || die "交互菜单需要终端，请改用子命令与参数（见 --help）"
}

ask_menu() {
  local title="$1" text="$2"
  shift 2
  local -a items=("$@")
  local -a keys=()
  local i=0 reply n
  printf '\n== %s ==\n%s\n' "${title}" "${text}" >&2
  while ((i < ${#items[@]})); do
    keys+=("${items[i]}")
    printf '  %d) %s\n' "$((i / 2 + 1))" "${items[i + 1]:-}" >&2
    i=$((i + 2))
  done
  while true; do
    read -r -p "请输入编号（回车返回）：" reply || return 0
    [[ -n ${reply} ]] || return 0
    if [[ ${reply} =~ ^[0-9]{1,3}$ ]]; then
      n=$((10#${reply}))
      if ((n >= 1 && n <= ${#keys[@]})); then
        printf '%s\n' "${keys[n - 1]}"
        return 0
      fi
    fi
    msg "无效编号：${reply}"
  done
}

ask_input() {
  local prompt="$1" default="${2:-}" value
  local hint="${prompt}${default:+ [${default}]}"
  read -r -p "${hint//\\n/$'\n'}：" value || true
  printf '%s\n' "${value:-${default}}"
}

ask_password() {
  local prompt="$1" value
  read -r -s -p "${prompt//\\n/$'\n'}：" value || true
  printf '\n' >&2
  printf '%s\n' "${value}"
}

ask_yesno() {
  local prompt="$1" default="${2:-n}" answer
  local mark="[y/N]"
  if [[ ${default} == "y" ]]; then
    mark="[Y/n]"
  fi
  local hint="${prompt} ${mark}"
  read -r -p "${hint//\\n/$'\n'}：" answer || true
  answer="${answer:-${default}}"
  [[ ${answer} == "y" || ${answer} == "Y" ]]
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
  prepare_etc
  local file="${ETC_DIR}/relay.toml"
  umask 077
  cat >"${file}" <<EOF
listen = "$(toml_escape "${listen}")"
psk = "$(toml_escape "${psk}")"
EOF
  if [[ ${local_enabled} == "1" ]]; then
    printf 'id = "%s"\n' "$(toml_escape "${id}")" >>"${file}"
  else
    printf 'local = false\n' >>"${file}"
  fi
  chmod 0600 "${file}"
  msg "已写入 ${file}"
}

write_agent_config() {
  local connect="$1" id="$2" psk="$3"
  prepare_etc
  local file="${ETC_DIR}/agent.toml"
  umask 077
  cat >"${file}" <<EOF
connect = "$(toml_escape "${connect}")"
id = "$(toml_escape "${id}")"
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
  local unit="dsh-remote-${role}.service"
  if [[ ! -f $(unit_path "${role}") ]]; then
    msg "${unit} 未安装，请先执行「配置并部署${role}」"
    return 1
  fi
  case ${action} in
    status) systemctl --no-pager --lines=20 status "${unit}" || true ;;
    logs)
      trap '' INT
      (trap - INT; journalctl -u "${unit}" -n 100 --no-pager -f) || true
      trap - INT
      ;;
    uninstall)
      systemctl disable --now "${unit}" >/dev/null 2>&1 || true
      rm -f "$(unit_path "${role}")"
      systemctl daemon-reload
      msg "已停止并移除 ${unit}（配置文件与二进制保留）"
      ;;
    *)
      systemctl "${action}" "${unit}"
      systemctl is-active "${unit}" >&2 || true
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
  local generated psk
  generated=$(gen_psk)
  if ask_yesno "是否自动生成 32 字节随机 PSK？\n（选否可手动输入，需与控制端/中继一致）"; then
    msg "已生成 PSK：${generated}"
    printf '%s\n' "${generated}"
    return 0
  fi
  psk=$(ask_password "请输入 PSK")
  [[ -n ${psk} ]] || die "PSK 不能为空"
  printf '%s\n' "${psk}"
}

configure_controller() {
  local listen relay_addr mcp_http psk
  listen=$(ask_input "监听地址（如 0.0.0.0:${DEFAULT_PORT}）" "0.0.0.0:${DEFAULT_PORT}")
  relay_addr=$(ask_input "可选：要接入的中继地址，留空表示纯直连模式")
  mcp_http=$(ask_input "可选：MCP Streamable HTTP 监听地址，留空表示不开启")
  psk=$(ask_psk)
  install_binary
  local relay_normalized=""
  if [[ -n ${relay_addr} ]]; then
    relay_normalized=$(normalize_addr "${relay_addr}")
  fi
  listen=$(normalize_addr "${listen}")
  write_controller_config "${listen}" "${relay_normalized}" "${mcp_http}" "${psk}"
  enable_service controller
  msg "部署完成；重新运行脚本选择「生成被控端一键接入命令」即可查看接入命令"
}

configure_relay() {
  local listen psk id local_enabled
  listen=$(ask_input "监听地址（如 0.0.0.0:${DEFAULT_PORT}）" "0.0.0.0:${DEFAULT_PORT}")
  local_enabled=0
  id=""
  if ask_yesno "是否允许控制端在中继本机上执行命令？\n（开启后中继本机会以节点形式出现在节点列表）" y; then
    local_enabled=1
    id=$(ask_input "中继本机作为节点时使用的标识" "$(hostname)")
  fi
  psk=$(ask_psk)
  install_binary
  listen=$(normalize_addr "${listen}")
  write_relay_config "${listen}" "${id}" "${psk}" "${local_enabled}"
  enable_service relay
  msg "部署完成；重新运行脚本选择「生成被控端一键接入命令」即可查看接入命令"
}

configure_agent() {
  local connect id psk
  connect="${AGENT_CONNECT:-}"
  [[ -n ${connect} ]] || connect=$(ask_input "控制端或中继地址（如 10.0.0.1:${DEFAULT_PORT}）")
  [[ -n ${connect} ]] || die "接入地址不能为空"
  id="${AGENT_ID:-$(hostname)}"
  psk="${AGENT_PSK:-}"
  [[ -n ${psk} ]] || psk=$(ask_password "请输入与控制端/中继一致的 PSK")
  [[ -n ${psk} ]] || die "PSK 不能为空"
  install_binary
  write_agent_config "$(normalize_addr "${connect}")" "${id}" "${psk}"
  enable_service agent
  msg "部署完成"
}

service_menu() {
  local role action
  role=$(ask_menu "服务管理" "选择角色" "${ROLES[@]}")
  [[ -n ${role} ]] || return 1
  action=$(ask_menu "服务管理 - ${role}" "选择操作" \
    status "查看状态" start "启动" stop "停止" restart "重启" logs "实时日志" uninstall "卸载服务")
  [[ -n ${action} ]] || return 1
  service_action "${role}" "${action}"
}

access_menu() {
  local role addr
  role=$(ask_menu "一键接入命令" "选择服务角色" "${ROLES[@]:0:4}")
  [[ -n ${role} ]] || return 1
  addr=$(sed -n 's/^listen = "\(.*\)"$/\1/p' "${ETC_DIR}/${role}.toml" | head -n1)
  [[ -n ${addr} ]] || die "读取 ${ETC_DIR}/${role}.toml 中的 listen 失败"
  print_access_command "${role}" "${addr}"
}

main_menu() {
  need_tty
  while true; do
    local choice
    choice=$(ask_menu "dsh-remote 一键配置" "请选择要执行的操作" \
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
  access [controller|relay]   打印被控端一键接入命令

agent 专用选项：
  --connect <地址>        控制端或中继地址
  --psk <密钥>            预共享密钥
  --id <节点标识>         节点标识，默认主机名

环境变量：
  DSH_REMOTE_SCRIPT_URL   本脚本的公网地址（用于生成接入命令）
EOF
}

ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
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

need_root

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
      need_tty
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
