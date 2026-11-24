## 简介
- 将任意 Linux 接入 dsh
- 本项目仅个人使用
## 传输
- 单条 TCP 长连接，PSK 握手双向校验，随后全程 ChaCha20-Poly1305 加密
- 连接内多路复用（yamux），控制流与业务流互不阻塞
## 一键脚本
```
bash <(curl -fsSL https://raw.githubusercontent.com/BanYeHanFeng/dsh-remote/dev/scripts/dsh-remote.sh)
```
