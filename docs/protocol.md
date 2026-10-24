# dsh-remote 协议

协议只服务 dsh-remote 自己的三个角色，目标是好实现、好排错、可演进。内部用
CBOR（`ciborium`）编码，文档里的示例写成 JSON 方便阅读。

## 1. 传输分层

```text
TCP
└── Noise_IKpsk2_25519_ChaChaPoly_BLAKE2s（每条连接独立握手）
    └── 帧：u16 密文长度（大端）+ Noise 密文（明文 = CBOR 编码的 Msg，`t` 标签即消息类型）
        └── 消息：Hello / Call / Result / Chunk / End / ...
```

- 握手与传输阶段都用 `u16` 长度前缀；Noise 单条消息上限 65535 字节，明文最大 65519。
- 应用层分片：`Chunk.data` 每片 60 KiB，CBOR 里编码为字节串（不是整数数组），
  连同头部仍小于明文上限；接收端按 `seq` 拼接，允许乱序到达后重排。
- 版本字段 `proto: 3` 放在 `Hello`/`Welcome`；不匹配由 hub 回 `Bye{code:"proto_mismatch"}`
  后断开。3 删掉了 `read`/`write`/`edit`，`bash` 改成单条 `Result`，不互通，三端需要
  一起升级。
- 每条连接有两条出站队列：`Chunk`/`End`/`Result` 走数据队列并按入队顺序发送，
  `Ping`/`Pong`/`Bye`/`Cancel` 等走控制队列优先发送。写线程按批合并帧
  （一次 `write_all` + 一次 `flush`），心跳不会排在成兆的数据后面。
- 所有整数大端；`req_id` 为 u64，连接内单调递增；时间戳为 Unix 毫秒。

## 2. 握手

```text
initiator                          responder
   │ ── e, es, s, ss {psk} ──────▶ │   IK 第一趟
   │ ◀─────── e, ee, se {psk} ──── │   IK 第二趟，双方完成认证
   │ ── Hello ───────────────────▶ │
   │ ◀────────────────── Welcome ── │
```

- 发起方静态公钥由 Key 派生，responder 校验其等于期望值；responder 的静态公钥
  同样由 Key 派生，发起方据此确认连到了持有同一 Key 的中继。
- 握手失败（Key 不同）不返回任何明文原因，只记本地日志并断开，避免探测。
- 握手完成后 5 秒内必须收到合法 `Hello`，否则断开。

## 3. 消息

### 3.1 控制

| type | 名称 | 方向 | 字段 |
| --- | --- | --- | --- |
| 0x01 | `Hello` | C→S | `proto, role, name, version, os, arch, roots[]` |
| 0x02 | `Welcome` | S→C | `proto, hub_name, targets[], server_online` |
| 0x03 | `Ping` | 双向 | `nonce` |
| 0x04 | `Pong` | 双向 | `nonce` |
| 0x05 | `Bye` | 双向 | `code, message` |
| 0x21 | `TargetEvent` | S→C | `kind: online\|offline, target` |

`role` 取 `server` / `agent`。`roots` 仅用于展示，实际限制在目标本地执行。

`Welcome.targets` 里 `hub` 自身也在列表中：

```json
{"name":"hub","kind":"hub","os":"linux","arch":"x86_64","version":"0.1.0",
 "since":1730000000000,"roots":["/data"]}
```

### 3.2 调用

| type | 名称 | 方向 | 字段 |
| --- | --- | --- | --- |
| 0x10 | `Call` | server→hub | `id, target, method, params` |
| 0x11 | `Result` | hub→server | `id, ok, value?, error?` |
| 0x12 | `Chunk` | 双向 | `id, seq, stream: 1\|2, data` |
| 0x13 | `End` | 双向 | `id, status, size?, sha256?` |
| 0x14 | `Cancel` | server→hub→agent | `id, reason?` |

- `method` 取工具名：`bash`、`upload`、`download`、`transfer`、`targets`、`shutdown`。
  `params` 是方法参数的 CBOR map，原样透传。
- `shutdown` 只有 agent 处理：先回 `Result{ok:true, value:{stopping:true}}`，
  然后断开中继、退出进程；hub 自身回 `unsupported`（中继不能被远程关掉）。
- 短结果用 `Result` 一次返回（`targets`、`bash`）。
- 长结果用流：先若干 `Chunk`，最后 `End`；目前只有文件传输用流，且只用 `stream=1`。
- `End.status` 取 `ok` / `error` / `cancelled`；文件传输带 `size` 与 `sha256`。
  下载的 `End` 是终态；上传的 `End` 只是上行数据结束，agent 还会回一条 `Result`。
- `seq` 在每个 `stream` 内独立从 0 递增，接收端按流分别拼接；重复或跳号即判定
  `protocol` 错误并 `Cancel`。
- `Call.id` 由 server 分配；hub 转发给 agent 前改写为房间内全局唯一的 `call_id`，
  回程再改写回 server 的 `id`。
- `Result` 与 `End` 都是终止消息：hub 收到后释放 `pending` 与超时计时器。
  例外是上传方向 server 发出的 `End`，它只表示数据发完，hub 保留 `pending`
  直到 agent 的 `Result`。
- 一条 `Call` 的任一阶段出错，都以 `Result{ok:false}` 或 `End{status:"error"}`
  收尾，保证请求不悬挂。

`bash` 在目标上跑在**同一个常驻 Shell** 里：`command` 由一段带随机标记的包装行写入
标准输入，输出（stdout 与 stderr 合并）按结束标记切出来，因此当前目录、导出的环境
变量等状态跨调用保留。参数只有 `command` 与内部使用的 `timeout_ms`。

```json
{"id":17,"target":"nas","method":"bash",
 "params":{"command":"df -h","timeout_ms":300000}}
```

```json
{"id":17,"ok":true,"value":{
  "output":"Filesystem ...\n[命令结束，退出码 0]",
  "exit_code":0,"timed_out":false}}
```

- `output` 已经按目标的 `MAX_OUTPUT_CHARS`（默认 16000 字符）截断，并附上
  `[命令结束，退出码 N]`；超时附 `[命令超时]`、Shell 退出附 `[Shell 已退出，退出码 N]`，
  两种情况都会重置 Shell。
- `timed_out` 与 `exit_code` 供调用方决定是否把这次工具调用标成失败。
- 单条 `Result` 编码后不超过 60 KiB，`output` 另按 48 KiB 兜底截断。

### 3.3 文件传输

上传（server → hub → agent）：

```text
server ── Call{id, target, method:"upload",
             params:{remote_path,size,overwrite,create_parents?}} ──▶
server ── Chunk{id, seq, stream:1} ... ──▶
server ── End{id, status:"ok", size, sha256} ──▶
agent  ── Result{ok:true, value:{path,size,sha256}} ──▶
```

- hub 为 server 发来的 `Chunk`/`End` 按 `(server_id, id)` 找到 `call_id` 后转发；
  不缓存内容，也不在上行 `End` 时释放 `pending`。`target` 就是 hub 自己时，
  这些消息直接交给进程内执行器的上传任务。
- agent 收到 `Call` 后先校验路径与覆盖策略，再在目标目录创建
  `.dsh-remote-<pid>-<rand>.part`；每个 `Chunk` 按 `seq` 顺序写入并累计 sha256。
- `Call.params.size` 用于提前拒绝超过 `MAX_FILE` 的请求；`End.size` 与
  `End.sha256` 是最终校验依据。校验失败或取消时删除临时文件。
- 校验通过后 `rename` 到目标路径；`overwrite=false` 时目标已存在直接回
  `conflict`。

下载（agent → hub → server）：

```text
server ── Call{id, target, method:"download", params:{remote_path,max_size}} ──▶
agent  ── Chunk{id, seq, stream:1} ... ──▶
agent  ── End{id, status:"ok", size, sha256} ──▶
```

- 下载没有额外的 `Result`，`End` 即终态。
- server 边收边写本地 `.part`，按 `seq` 拼接并累计 sha256，校验 `size` 后原子
  rename；失败、取消或超时删除 `.part`。
- agent 在打开文件前检查自身的 `MAX_FILE` 与请求里的 `max_size`，取较小值。

节点互传（源目标 → hub → 目的目标）：

```text
server ── Call{id, method:"transfer",
             params:{source,source_path,dest,dest_path,overwrite?,create_parents?,max_size?}} ──▶
hub    ── Call{dest_call, target:dest, method:"upload",
             params:{remote_path:dest_path,size:max_size,overwrite?,create_parents?}} ──▶
hub    ── Call{src_call, target:source, method:"download",
             params:{remote_path:source_path,max_size}} ──▶
source ── Chunk{src_call, seq, stream:1} ──▶ hub ── Chunk{dest_call, seq} ──▶ dest
source ── End{src_call, status:"ok", size, sha256} ──▶ hub ── End{dest_call, …} ──▶ dest
dest   ── Result{ok:true, value:{path,size,sha256}} ──▶ hub ── Result ──▶ server
```

- `transfer` 只有 hub 处理，agent 收到回 `unsupported`。hub 把它拼成两条普通调用，
  自己不落盘、不缓存整文件，只按 `seq` 重编号后转发分块，所以 DSH 本机完全不参与数据。
- 先后顺序：先给目的端发 `upload`（它会先校验路径、覆盖策略并创建 `.part`），
  再给源端发 `download`，这样目的端失败时不会有数据被白读。
- 任一腿失败或取消，hub 取消另一条腿并回一条 `Result{ok:false}`，请求不会悬挂。
- 分块有背压：hub 给一侧投递超过 10 秒不成功即判定这次传输失败并取消两条腿
  （`busy`），不会静默丢块。
- `max_size` 同时用作源端读取上限与目的端 `Call.params.size`（提前拒绝超限 + 估算超时）。
- 源端或目的端可以就是 hub 自己，此时那一条腿走进程内执行器，路由与取消逻辑不变。

分块与结果上限：

- `Chunk.data` 每片 60 KiB，`stream` 固定为 1；`seq` 从 0 连续递增。
- 接收端按 `seq` 重排：连号即落盘并累计 sha256，超前分片先缓存（最多 512 片），
  收尾时仍有空洞就判定失败并丢弃临时文件。
- `Result` 编码后不超过 60 KiB；更大的数据用上传下载。

## 4. 路由

hub 内维护：

- `agents: Map<name, Conn>`，注册时查重；断开即移除并广播 `TargetEvent{offline}`。
- `servers: Set<Conn>`，允许多个并存（DSH 探测进程、多个 DSH 实例），事件广播给全部。
- `routes: Map<call_id, Pending>` + `legs: Map<leg_call_id, 根 call_id>`：
  单路调用只有一条腿，互传有源和目的两条腿，回程先按腿找到根、再改回 `server_req_id`。

处理一条 `Call`：

1. 校验来源是 `server`，否则记日志忽略。
2. `method == "targets"` 或 `target == hub_name` → 本地应答，不转发。
3. 查 `agents`：不存在回 `Result{error:{code:"unknown_target"}}`。
4. 分配 `call_id`，记录 `pending`，转发给 agent，设置截止时间。
5. server 后续的 `Chunk`/`End`（上传）按 `(server_id, req_id)` 找到 `call_id` 后
   转发给 agent；上行 `End` 不释放 `pending`。
6. agent 的 `Result`/`Chunk`/`End` 按 `call_id` 找到发起方，改写 `id` 后回传。
7. 超时：回 server `Result{error:{code:"timeout"}}`，给 agent 发 `Cancel`。
8. agent 断开：其所有 `pending` 回 `offline`；server 断开：其所有 `pending` 给
   agent 发 `Cancel`。

中继自身也是一个**进程内 agent**（目标名与中继同名，`kind: hub`）：它由一条内存
通道接到同一个 `Executor` 上，因此本地调用与远程调用走同一条路由、同一套取消与
超时逻辑，只是不需要网络。`targets` 例外，由中继直接读自己的注册表回答。

server 侧每个调用有独立的有界投递队列：连接读循环只做非阻塞入队，慢调用不会拖住
其它调用；队列积压超过 512 条视为调用方停滞，回 `Cancel` 并放弃该调用。
连接断开时所有队列立即关闭，等待中的调用马上拿到“连接中断”而不是空等到超时。
中继侧不为每条调用起计时器，而是由单个每秒扫描的任务清理超时项。

hub 不缓存 `Chunk` 内容，只做转发；它必须能读到 `method`、`target`、`req_id`
用于路由和审计。这正是“逐段加密”的边界。

## 5. 心跳、超时与重连

| 项 | 值 |
| --- | --- |
| 心跳空闲间隔 | 15s |
| 判定断线 | 45s 内无任何帧（含心跳） |
| 握手超时 | 10s |
| `Hello` 超时 | 5s |
| 默认调用超时 | 60s；`bash` 为 `timeout_ms + 30s`；文件传输与节点互传为 `300s + 大小/256KiB + 10s`，封顶 1 小时（三端共用 `exec::limits` 一处定义） |
| 重连退避 | 1s 起翻倍到 30s，加 0–20% 抖动 |

- server 与 agent 断线后无限重连；进程不退出（除非 stdin EOF / SIGTERM）。
- 重连成功后重新 `Hello`，`Welcome.targets` 覆盖本地快照。
- server 重连时若发现同名 agent 已换连接，后续调用直接走新连接，无需重启。
- 同名 agent 重连时顶替旧连接：hub 先接受新连接并注册，再给仍被认为是
  在线（TCP 未关但已失联）的旧连接发 `Bye{code:"replaced"}` 并立即回收，
  不必等对方 45s 心跳超时。旧连接名下的 `pending` 立即回 `offline`。
- 被顶替的 agent 收到 `replaced` 后按 60s + 0–15s 抖动重试，避免两个同名
  实例互踢；这通常意味着重复启动了同一目标。

## 6. 错误码

| code | 场景 |
| --- | --- |
| `bad_key` | 握手 PSK 不匹配（仅本地日志，不回明文） |
| `duplicate_name` | agent 名与中继自身名字冲突 |
| `replaced` | hub 通知旧连接：同名目标已由新连接顶替 |
| `unknown_target` | 目标名不存在（含刚下线） |
| `offline` | 目标在线但连接已断 |
| `timeout` | hub 或本地执行超时 |
| `cancelled` | 调用方主动取消 |
| `denied` | 路径越界、bash 被禁用、权限不足 |
| `not_found` | 路径不存在 |
| `conflict` | 目标已存在且未允许覆盖 |
| `too_large` | 输出或文件超上限 |
| `busy` | 并发执行数达到上限 |
| `io` | 本地文件/进程 IO 错误 |
| `protocol` | 帧或消息不合法（含 `seq` 跳号） |
| `unsupported` | 目标不支持该方法（例如 agent 上的 `targets`） |
| `internal` | 未分类错误 |

`Result.error` 结构：`{code, message}`。`message` 面向模型，中文，不回显绝对路径。

## 7. MCP 映射

对外只有 MCP 一种入口（`dsh-remote server`），工具名与内部 `Call.method` 一一对应，
参数用 JSON 名称，服务端解析成 CBOR 参数结构后透传（CLI 只保留 `targets` 用于连通性自检）：

| 工具 | 内部 method | 终态消息 | MCP 侧超时 |
| --- | --- | --- | --- |
| `targets` | `targets`（发给 hub） | `Result` | 用本地快照，断线也应答 |
| `bash` | `bash` | `Result` | `300s + 30s`（目标可用 `SHELL_TIMEOUT_MS` 调小） |
| `upload` | `upload`（本机→节点）或 `transfer`（`from` 节点互传） | 上行 `Chunk`+`End`，回程 `Result` | `300s + 大小/256KiB` |
| `download` | `download`（节点→本机）或 `transfer`（`to` 节点互传） | 下行 `Chunk`+`End`，或互传的 `Result` | `300s + 大小/256KiB` |
| `actions stop`（内部） | `shutdown`（发给目标节点） | `Result` | 10s |

- `req_id` 由 server 侧的 `Session` 单调分配，一条连接内并发任意多路；
  收到 `End`/`Result` 即释放。
- MCP 的 `notifications/cancelled` → 内部 `Cancel{id}`，hub 再转发给 agent：
  bash 杀进程组，传输删临时文件。
- 错误码原样透出（`unknown_target`、`offline`、`timeout`、`denied` 等），
  MCP 里表现为 `isError: true` 加一行 `错误 [code]: message`。
- 中继断开时 `pending` 中的调用统一回 `offline`，`targets` 返回未连接的空列表；
  重连后 `Welcome.targets` 覆盖本地快照，不需要重启 DSH。

## 8. 演进

- 新增消息类型不改变已有语义；未知 type 记录日志后忽略，连接不断开。
- `Hello.proto` 只做“同主版本可通信”的粗校验；字段用 CBOR 可选字段扩展。
- 端到端加密演进：把 `Call.params` 与 `Chunk.data` 换成 server 与 agent 协商的
  不透明密文，hub 只保留 `target`、`req_id`、`method` 用于路由；消息表不推翻。
- MCP 层协议版本走 MCP 自己的 `initialize` 协商，与内部协议版本无关。
