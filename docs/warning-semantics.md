# 预警修订与区域影响

本文件说明接收程序之上的业务解释：如何把预警正文、区域版本和有效修订拼成
有依据的事件，如何在不猜范围的前提下计算订阅者影响，以及通知差异如何在重启
与重试后保持一致。原始响应的接收与归档见 `source-protocol.md`。

## 事件身份与证据

来源记录的身份是 `issuer`、`alert`、`revision`。同一事务内（发行方 + 事件）
按修订号推进，修订号大者为有效修订，但不以“最后收到”作为“最新”。

一次预警的构成分三部分，各自保留为证据：

- **正文**：`body`/`text` 原样保存，只供值班员阅读和核对，不参与范围解析。
- **区域版本**：`boundary_version` 指定范围代码的含义。它是一个显式引用，
  缺失或未知时只报告缺口，不回退到“今天”的边界。
- **有效修订**：`revision` 与 `status`（`active`/`cancelled`）共同决定事件
  当前状态。

同一来源同一修订可由多份观测片段（如正文与稍后到达的范围文件）合并。合并
只接受一致的结构化字段；同一字段出现不同有效值会记为冲突并阻止应用，而不是
取其中一份。

## 区域代码只在边界版本内有效

`boundaries.py` 保存不可变的行政区快照。每个 `boundary_version` 下：

- `code`：来源引用的区域代码，只在所属版本内有效；
- `area_id`：跨版本稳定的实际区域标识，用于判断“是否同一片地方”；
- `parent`：同版本内的父代码；子区域在影响计算中随父区域一并覆盖。

行政区调整后，同一 `code` 可以在新版本中指向不同 `area_id`。因此：

- 来源声明的范围只在它自己的 `boundary_version` 内解析；
- 订阅者按登记时的边界版本解析其区域；
- 两版之间以 `area_id` 对齐，而不是以 `code` 对齐。

例如 `town-99` 在 `2026-a` 是甲县西镇（`area-west`），在 `2026-b` 被复用
为乙县东镇（`area-east`）。登记在 `2026-a` 的 `sub-west` 只受前者影响，
登记在 `2026-b` 的 `sub-east` 只受后者影响。

## 缺资料时显示缺什么

解析结果要么完整，要么明确列出缺失项，绝不从正文推断范围。面向值班员的
标识包括：

| 标识 | 含义 |
| --- | --- |
| `boundary:<版本>` | 事件引用了尚未收到的边界版本 |
| `region:<代码>@<版本>` | 区域代码在该版本内不存在 |
| `subscriber-boundary:<版本>` | 订阅者登记的边界版本缺失 |
| `subscriber-region:<代码>@<版本>` | 订阅者登记的代码在该版本内不存在 |
| `subscribers` | 尚未收到订阅者花名册 |
| `missing_field:<字段>` / `invalid_field:<字段>` | 观测缺少或写坏了结构化字段 |
| `conflict:<来源>/<事件>/r<修订>/<字段>` | 同一修订的片段自相矛盾 |

缺资料的修订不会被应用，评估停在该修订，等资料补齐后从同一位置继续。已经
应用的修订不会因重启而重放。

## 有效修订与通知差异

只有完全确定的影响才会改动通知队列。对每个订阅者，按“上一已应用修订 →
当前修订”比较：

- 新受影响且从未收到通知：发出 `warning`；
- 仍受影响但范围或边界版本变化：发出 `update`；
- 仍受影响且完全未变：不重复发送；
- 不再受影响且旧通知尚未发出：直接 `withdrawn` 撤下；
- 不再受影响且旧通知已经发出：生成 `correction`，其 `correction_of` 指向
  原通知，并在 `payload.original` 中保留原通知的修订、边界版本与覆盖区域；
- 撤销（`status=cancelled`）：所有曾收到通知的订阅者得到关联更正。

每条通知都有去重键，例如 `weather-a|RAIN-5|sub-west|warning@2` 或
`weather-a|RAIN-5|sub-west|correction@4->3`。同一修订的重复评估、采集重试
或进程重启只会复用已有通知，不会重复发出一轮更正。

## 两家来源结论不一致

不同 `issuer` 各自独立推进，互不覆盖。若最新已应用结论在状态、边界版本或
覆盖区域上不同，报告中的 `conflicts` 会以 `source-disagreement` 保留每家
来源的结论，交由值班员处置；程序不会替值班员择一。

## 持久状态与恢复

`store.py` 的 SQLite 状态库保存：观测片段、边界快照、花名册、每个来源已应用
到的修订（前沿）、通知及其事件日志、每次评估。原始响应仍在 `archive` 的
只追加库中。

恢复只需重新打开同一个状态库：

```sh
python3 -m weather_bulletins resolve  --archive raw.sqlite3 --state state.sqlite3 --alert RAIN-5
python3 -m weather_bulletins evaluate --archive raw.sqlite3 --state state.sqlite3 --alert RAIN-5
python3 -m weather_bulletins notify   --archive raw.sqlite3 --state state.sqlite3
python3 -m weather_bulletins report   --archive raw.sqlite3 --state state.sqlite3 --alert RAIN-5
```

`report` 按修订列出每个订阅者是否受影响、收到哪一版通知、该通知当前是已发、
已撤还是更正，并保留未决缺口与来源分歧。

## 演示

`python3 -m weather_bulletins demo` 在临时目录中启动本地来源，依次演示整县
扩大、缩小到两镇、再撤销，输出每一步的通知差异与最终订阅者时间线。所有条目、
机构与回执均为本地合成资料。
