# 气象预警修订的区域影响重算

程序分两层：底层按原始格式接收来源响应并只追加归档；业务层把预警正文、区域
版本和有效修订装配成有依据的事件，在声明过的行政边界快照内计算订阅者影响，
再向通知队列发布变更。区域代码不会跨版本混用，配套资料没到齐时只报告缺什么，
不凭正文猜范围。

## 运行环境

使用 Python 3.11 及以上，业务运行依赖均为标准库；SQLite 由 Python 自带。时区资料使用运行环境内的系统数据库。运行过程不安装依赖，也不下载数据。本地来源服务器和接收程序在同一进程环境中即可运行。

构建工具及其传递依赖精确锁定在 `requirements-build.txt`。首次准备开发环境时先执行下面命令；之后测试、构建和演示均可离线运行，不在运行命令中安装包。

```sh
python3 -m pip install -r requirements-build.txt
```

```sh
python3 -m unittest discover -s tests -v
python3 -m build --wheel --no-isolation
python3 -m compileall -q weather_bulletins tests
python3 -m weather_bulletins demo
```

演示从空的临时目录启动本地来源，真实完成 HTTP 获取与归档，并依次输出“整县
扩大 → 缩小到两镇 → 撤销”每一步的通知差异，以及最终按修订排列的订阅者影响
时间线；结束自动清理。本地测试、编译和演示合计一般不超过十秒，可直接展示，
不依赖历史日志。

## 常驻来源与原始接收

```sh
python3 -m weather_bulletins serve
```

终端会输出本次分配的回环地址。在另一终端将下面地址替换为实际地址：

```sh
python3 -m weather_bulletins capture --base-url http://127.0.0.1:实际端口 --archive .runtime/raw.sqlite3 --source local-source --path /feed
```

归档库只追加来源观察，不用最后一条响应覆盖先前响应。`archive.py` 管理原始内容及完整性，`client.py` 执行一次请求，`decoding.py` 负责来源格式解码，`origin.py` 提供协议演练。

## 业务处理与通知差异

同样的采集结果进入业务状态库后，可以分阶段推进，也支持进程重启后继续：

```sh
python3 -m weather_bulletins pull     --archive .runtime/raw.sqlite3 --state .runtime/state.sqlite3 --base-url http://127.0.0.1:实际端口 --source weather-a --path /feed
python3 -m weather_bulletins resolve  --archive .runtime/raw.sqlite3 --state .runtime/state.sqlite3 --alert RAIN-5 --materials fixtures/materials.json
python3 -m weather_bulletins evaluate --archive .runtime/raw.sqlite3 --state .runtime/state.sqlite3 --alert RAIN-5
python3 -m weather_bulletins notify   --archive .runtime/raw.sqlite3 --state .runtime/state.sqlite3
python3 -m weather_bulletins report   --archive .runtime/raw.sqlite3 --state .runtime/state.sqlite3 --alert RAIN-5
```

- `boundaries.py`：不可变的行政区快照；区域代码只在 `boundary_version` 内
  有效，跨版本以稳定的 `area_id` 对齐。
- `warning.py`：把来源记录规范化为观测片段，保留正文作为证据，不解析地名。
- `store.py`：观测、配套资料、每个来源的已应用修订、通知及其事件日志。
- `engine.py`：装配有效修订、计算影响、生成可去重的通知差异、出报表。
- `pipeline.py`：把上述阶段串成可中断、可恢复的流程。

详细语义见 `docs/warning-semantics.md`。

## 开发资料

来源协议见 `docs/source-protocol.md`，业务语义见 `docs/warning-semantics.md`，
领域观察关系见 `tests/test_source_contract.py`。`fixtures/materials.json` 提供
两版行政边界与订阅者花名册，其中 `town-99` 在两版中指向不同实际区域；
`fixtures/business_cases.json` 保存业务侧关注的交错情况及可观察口径。所有条目、
机构、文件内容和门户回执均为本地合成资料，不包含真实用户身份。

## Wheel 分发

构建产物位于 `dist/`，包含 `weather_bulletins` 和命名空间内的合成来源 JSON
及配套资料。安装 wheel 后，即使当前目录不是源码目录，也可执行
`python3 -m weather_bulletins demo`。提供 `serve --scenario` 时仍以指定文件为准，
找不到指定文件会直接报错，不回退到默认资料。
