# 气象预警修订的区域影响重算

本程序接收来源接口，保留响应状态、头部、原始内容与每次观察的时间，然后把
预警正文、区域版本与有效修订拼成有依据的事件，计算应向通知队列发布的变更。

它能回答值班员关心的几件事：

* **现行结论**：同一来源以声明的最大修订号为准，即使采集顺序相反（先拿到
  旧正文、后拿到新版范围），也取最新修订。
* **范围只看资料**：影响范围由“区域代码 + 该记录声明的边界版本”解析得到。
  同一个区域代码在不同版本中可以有不同覆盖地点（行政区边界会调整），因此
  永远使用记录自带的 `boundary_version`，不按“当前版本”覆盖，也不按正文猜。
* **缺什么就说什么**：边界版本或区域代码未到齐时，影响范围记为未确定，通知
  暂不发出，报告明确列出缺少的版本或代码（`materials_missing`）。
* **撤销与更正**：撤销发生后，未发送的通知转为 `retracted`；已发送的通知生成
  与原通知关联的更正（`link_id` 指向原通知）。范围缩小时同样会给被排除的
  订阅者生成关联更正。采集重试按业务键去重，不会重复发出一轮更正。
* **来源分歧**：两家来源对同一事件说法不一致时，事件标记为 `disputed`，各自
  结论并列保留，交给值班员处置；资料没到齐的来源记为 `pending`，不当成一致。
* **重启续做**：原始观察只追加，业务投影与通知状态存于同一 SQLite 文件，进程
  重启后继续收资料、比较通知差异。

## 运行环境

使用 Python 3.11 及以上，业务运行依赖均为标准库；SQLite 由 Python 自带。运行
过程不安装依赖，也不下载数据。本地来源服务器和接收程序在同一进程环境中即可
运行。

构建工具及其传递依赖精确锁定在 `requirements-build.txt`。首次准备开发环境时
先执行下面命令；之后测试、构建和演示均可离线运行，不在运行命令中安装包。

```sh
python3 -m pip install -r requirements-build.txt
```

```sh
python3 -m unittest discover -s tests -v
python3 -m build --wheel --no-isolation
python3 -m compileall -q weather_bulletins tests
python3 -m weather_bulletins demo
```

演示从空的临时目录启动本地来源，按真实交错顺序重放一次完整预警：先小范围、
再扩大、再缩小、最后撤销，逐步展示每个订阅者受到哪一版影响；另有一条预警展示
两家来源结论不一致时并列保留。结束时自动清理。本地测试、编译和演示合计一般
不超过十秒，可直接展示，不依赖历史日志。

## 采集并处理

`capture` 只做原始接收与归档；`ingest` 在同一进程里完成采集、写入业务状态、
按最新修订计算通知差异。两者都只追加来源观察，不用最后一条响应覆盖先前响应。

```sh
python3 -m weather_bulletins serve
```

终端会输出本次分配的回环地址。在另一终端：

```sh
python3 -m weather_bulletins ingest \
  --base-url http://127.0.0.1:实际端口 \
  --archive .runtime/raw.sqlite3 \
  --state .runtime/state.sqlite3 \
  --source local-source --path /feed \
  --subscribers fixtures/subscribers.json \
  --boundaries fixtures/boundaries
```

`--boundaries` 可给出多个文件或目录，目录中只读取 `*.json`。每个文件是一个边界
版本：

```json
{"version": "2026-b", "regions": [{"code": "town-11", "name": "合成镇十一", "covers": ["place-n", "place-e"]}]}
```

`covers` 是不可再分的稳定地点标识，订阅者也按这些地点声明覆盖范围。这样同一
区域代码跨版本调整边界时，影响范围仍可比较。

查看事件报告、或把一条待发送通知标记为已发送：

```sh
python3 -m weather_bulletins show --state .runtime/state.sqlite3 --event RAIN-5
python3 -m weather_bulletins send --state .runtime/state.sqlite3 --notification 1
```

`show` 输出每个来源的结论、缺少的资料、按订阅者列出的时间线与通知及其目标。
`send` 只在 `pending` 时生效，重复调用不会重发。

## 模块划分

`archive.py` 管理原始内容及完整性，`client.py` 执行一次请求，`decoding.py` 负责
来源格式解码，`origin.py` 提供协议演练。业务侧：`boundaries.py` 校验与解析区域
版本，`domain.py` 规范化来源记录，`store.py` 保存业务状态，`timeline.py` 计算
订阅者影响，`engine.py` 拼装事件并生成通知差异，`report.py` 汇总给值班员。

## 开发资料

来源协议见 `docs/source-protocol.md`。`fixtures/business_cases.json` 记录业务侧
关注的交错情况及可观察口径，`tests/test_business.py` 验证这些能力。所有条目、
机构、地点、文件内容和门户回执均为本地合成资料，不包含真实用户身份。

## Wheel 分发

构建产物位于 `dist/`，包含 `weather_bulletins` 与命名空间内的合成资料（含
`boundaries/`）。安装 wheel 后，即使当前目录不是源码目录，也可执行
`python3 -m weather_bulletins demo`。提供 `serve --scenario` 时仍以指定文件为准，
找不到指定文件会直接报错，不回退到默认资料。

