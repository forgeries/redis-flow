# redis-flow

[English](README.markdown) | 简体中文

redis-flow 是一个基于 Redis 的轻量级 Python 任务流程库。
通过 `yield Task(...)` 创建后续任务，工作线程负责执行回调。
内存缓冲区将写入合并成批次，减少 Redis 请求。
Redis 保留任务，直到工作线程确认任务完成。

项目当前处于 0.x 阶段，同一个任务可能执行多次。
请确保重复执行业务操作不会产生错误结果。
如果进程在缓冲区首次写入 Redis 前停止，初始任务可能丢失。
使用 `flow.submit(tasks)` 可在继续执行前等待 Redis 确认。
详细说明见[可靠性与限制（英文）](docs/reliability.markdown)。

## 安装

PyPI 发行名为 `redis-flow`，Python 导入名仍为 `rf`：

```sh
python -m pip install redis-flow
```

```python
from rf import Task, TaskFlow
```

本地开发时，可按下文从源码安装。
PyPI 上另一个名为 `rf` 的项目使用相同导入名，不要在同一环境中同时安装两者。

## 从源码快速开始

需要 Python 3.10 或更新版本，以及一个 Redis 单机服务。
在仓库根目录执行以下命令：

```sh
python -m venv .venv
```

激活虚拟环境：

```sh
# Linux or macOS
source .venv/bin/activate
```

```powershell
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

安装项目，启动开发用 Redis，再运行示例：

```sh
python -m pip install -e .
docker run --rm -d --name redis-flow-dev -p 127.0.0.1:6379:6379 redis:7-alpine
python -m examples.quickstart
```

输出中应包含 `Hello, redis-flow!`。
程序会在任务完成、完成记录写入 Redis 后退出。
使用结束后，执行 `docker stop redis-flow-dev` 停止容器。
该容器仅用于开发，没有配置生产环境所需的持久化方案。

## 定义任务流程

```python
from rf import Task, TaskFlow


class HelloFlow(TaskFlow):
    def start_jobs(self):
        yield Task(callback=self.greet, meta={"name": "redis-flow"})

    def greet(self, name):
        print(f"Hello, {name}!")


if __name__ == "__main__":
    flow = HelloFlow(redis_key="hello-flow")
    flow.start()
    try:
        while flow.is_alive():
            flow.join(0.2)
    except KeyboardInterrupt:
        flow.stop()
        while flow.is_alive():
            flow.join(0.2)
```

默认 Redis 地址为 `127.0.0.1:6379`，数据库编号为 `0`。
可通过 `REDIS_IP_PORTS`、`REDIS_USER_PASS` 和 `REDIS_DB` 环境变量修改连接配置。
也可以使用 Python 能导入的 `settings.py` 模块。
全部配置项见[默认配置](rf/default.py)。
不要提交真实密码或其他连接凭据。

## 提交并确认一批任务

生产者需要等待 Redis 接收任务时，使用 `submit()`：

```python
flow = HelloFlow(redis_key="hello-flow", auto_start_jobs=False)
receipt = flow.submit([Task("greet", {"name": "Alice"})])
assert receipt.is_set()
```

这会写入任务，但不会启动工作线程；需要另行启动流程来处理任务。
`submit()` 接受发往当前流程队列的 `Task` 对象。注册了多个处理类时，需指定 `process_name`。
每批任务必须能放入剩余缓冲容量。每个缓冲区的默认容量为 1,000 条记录。
容量不足时抛出 `BufferFullError`，整批新任务均不接收；任务数据无效时也会整批拒绝。
Redis 写入失败会抛出异常，但已接收的任务仍保留以便重试；写入也可能已经到达 Redis。
连接恢复后，可调用 `flow.flush()` 重试保留的写入，即使尚未启动工作线程也可以。
流程运行期间，`submit(tasks, wait=False)` 返回一个事件，Redis 确认该批任务后事件才会置位。
停止流程之前，应先停止外部生产者。
Redis 重启后的数据持久性仍取决于持久化设置。写入确认不表示任务已经执行完成。
完整程序见[提交示例](examples/submission.py)。

## 任务执行流程

1. `start_jobs()` 创建初始任务，并将任务放入缓冲区。
2. 缓冲区批量写入 Redis。采集器以原子操作领取任务，并设置租约截止时间。
3. 工作线程调用任务方法。该方法可以通过 `yield` 产生 `Task` 或 `Queue` 对象。
4. 任务缓冲区写入输出并确认任务完成。发往其他队列的输出必须先成功写入目标队列。
5. 心跳持续续租，直到 Redis 确认任务完成。进程停止后，其他工作线程可在租约到期后重新领取任务。

如需使用多个处理类，可继承 `BaseProcess`，并通过 `flow.add_process(...)` 注册各个类。
注册了不同处理类的工作程序应使用独立的 `redis_key` 命名空间，避免领取无法处理的任务。
条件分支、跨队列传递和每日定时运行的用法见[示例指南（英文）](examples/README.markdown)。

## 开发与检查

```sh
python -m pip install -e ".[dev]"
python -m ruff check .
python -m ruff format --check .
python -m pytest tests/unit -q
```

Redis 集成测试、打包检查和发布要求见[贡献指南（英文）](CONTRIBUTING.markdown)。
版本变化及 API 迁移说明见[更新日志（英文）](CHANGELOG.markdown)。

## 许可证与发布状态

本项目采用 [MIT 许可证](LICENSE)。
PyPI 发布通过手动启动的工作流执行，使用 Trusted Publishing 授权。
配置和发布步骤见[发布指南（英文）](docs/publishing.markdown)。
