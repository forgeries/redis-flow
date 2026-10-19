"""Run a minimal task flow: python -m examples.quickstart"""

import argparse

from examples._runner import run
from rf import Task, TaskFlow


class HelloFlow(TaskFlow):
    def __init__(self, name="redis-flow", **kwargs):
        super().__init__(**kwargs)
        self.greeting_name = name

    def start_jobs(self):
        yield Task(callback=self.greet, meta={"name": self.greeting_name})

    def greet(self, name):
        print(f"Hello, {name}!")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--name", default="redis-flow", help="Name to use in the greeting."
    )
    parser.add_argument(
        "--redis-key", default="example:hello", help="Task namespace for this example."
    )
    args = parser.parse_args()
    run(HelloFlow(name=args.name, redis_key=args.redis_key, thread_count=2))


if __name__ == "__main__":
    main()
