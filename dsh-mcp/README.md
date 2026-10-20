# dsh-mcp

从 DSH 安装目录复制出来的两个 MCP 插件，放在 dsh-remote 仓库里本地维护：

| 目录 | 包名 | 来源 | 作用 |
| --- | --- | --- | --- |
| `dsh-mcp-client/` | `@local/dsh-mcp-client` | `@deepseek-ai/dsh-mcp-client@0.2.1-rc.2` | 连接 MCP 服务端（`mcp-relay` 行），把服务端的工具注册成 `mcp__<server>__*` |
| `dsh-mcp-resources/` | `@local/dsh-mcp-resources` | `@deepseek-ai/dsh-mcp-resources@0.2.1-rc.2` | 提供 `mcpResources` 服务，并注册 `list_mcp_resources` / `list_mcp_resource_templates` / `read_mcp_resource` 三个共享工具 |

`lib/index.js` 是上游发布包里的构建产物（上游仓库 `packages/mcp/*` 的源码编译结果），
直接改这里的文件即可；`lib/types/`、`locale/`、README 原样保留，方便对照。

## 为什么复制

三个 MCP 资源工具由 `dsh-mcp-resources` 无条件注册（不看服务端的 capabilities），
只要有一条 MCP 连接就会出现，宿主侧没有开关。把这两个包搬进本仓库后，可以按需改：
例如删掉 `dsh-mcp-resources/lib/index.js` 里 `registerResourceTools` 的三个
`ctx.tools.register(...)`，三个工具就彻底不存在，任何预设都看不到。

## 安装到 DSH（profile）

在 profile 里用 link 方式安装（与 personal-minimal 的插件一致）：

```bash
dsh plugin --profile web add link:/root/github/dsh-remote/dsh-mcp/dsh-mcp-client
dsh plugin --profile web add link:/root/github/dsh-remote/dsh-mcp/dsh-mcp-resources
```

然后在 `/root/.dsh/profiles/web/cordis.patch.yml` 里把两处引用指向本地包。

注意 patch 条目的 `name` 只做校验（与目标行不一致会跳过），**不能改包名**，所以
dsh-base 里那行 `mcp-resources` 只能按 id 禁用，再 insert 一行本地包：

```yaml
# dsh-base bundle 里那行：禁用，改用下面的本地包
- id: mcp-resources
  disabled: true

- insert:
    # 本地 dsh-mcp-resources（提供 mcpResources 服务与三个资源工具）
    - id: mcp-resources-local
      name: '@local/dsh-mcp-resources'
    # 连接 relay 的那行，换成本地 dsh-mcp-client
    - id: mcp-relay
      name: '@local/dsh-mcp-client'
      config:
        serverName: relay
        transport: stdio
        command: /usr/local/bin/dsh-remote
        args: ["server", "--config", "/etc/dsh-remote/server.conf"]
        toolCallTimeoutMs: 660000
```

改完用 `dsh --profile web --dump-config` 检查（应能看到 `mcp-resources` 带
`disabled: true`，以及 `mcp-resources-local` / `mcp-relay` 指向 `@local/*`），
再重启 `dsh web`。

## 依赖解析的约定

本目录的包以 **link** 方式安装，DSH 在链接包里只按 `peerDependencies` 把裸包名路由到
安装目录（`dsh-app-boot` 的 `routeLinked`）：

- 所有运行时 import 的包都必须写在 `peerDependencies`，**不能写 `dependencies`**，
  否则解析失败；
- `@modelcontextprotocol/client`、`@deepseek-ai/schemastery`、`@deepseek-ai/dsh-util-values`
  虽然在上游是 `dependencies`，这里也一并挪到了 `peerDependencies`；
- 新增 import 时同步加一条 peer，并确认 DSH 安装目录里有那个包。

## 与上游的关系

- 包名改成了 `@local/*`，`private: true`，去掉了 `publishConfig`/`repository`/`devDependencies`。
- 代码本身是原样副本；本地改造记得记在这里，方便下次升级时重新对照。
- 升级 DSH 时，从新版本的
  `/usr/local/lib/node_modules/@deepseek-ai/dsh/node_modules/@deepseek-ai/dsh-mcp-*`
  重新复制 `lib/index.js`，再把本地改造补回去。
