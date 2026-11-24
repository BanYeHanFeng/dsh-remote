## 简介
- 将任意 Linux 接入 dsh
- 本项目仅个人使用
## 传输
- 单条 TCP 长连接，PSK 握手双向校验，随后全程 ChaCha20-Poly1305 加密
- 连接内多路复用（yamux），控制流与业务流互不阻塞
## 中继
- 被控端与控制端都接入中继后由中继转发；同一中继上的两台机器互传文件时，数据由中继直接搬运，不经控制端
- 中继本机默认也作为一个节点出现，控制端可以直接在它上面执行命令、上传下载文件
- 本机节点标识用 `--id` 指定（默认主机名），`--tag` 打标签；`--no-local` 或配置 `local = false` 可关闭该能力
- 同名被控端接入时远端优先，中继本机节点让位
## 一键脚本
```
bash <(curl -fsSL https://raw.githubusercontent.com/BanYeHanFeng/dsh-remote/dev/scripts/dsh-remote.sh)
```
## 发布
- 仅手动触发 `.github/workflows/release.yml`，版本号为 `v<日期>-<短哈希>`，可选预发布或正式发布；预发布只保留最新一个
- 产物：`dsh-remote-linux-x86_64`、`dsh-remote-linux-aarch64`、`SHA256SUMS`
- 一键脚本可直接拉发布产物：`DSH_REMOTE_BIN_URL=https://github.com/BanYeHanFeng/dsh-remote/releases/latest/download/dsh-remote-linux-x86_64`
