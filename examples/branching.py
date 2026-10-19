"""Branch and merge tasks: python -m examples.branching --count 6"""

import argparse

from examples._runner import positive_int, run
from rf import Task, TaskFlow


class InspectionFlow(TaskFlow):
    def __init__(self, count=6, **kwargs):
        super().__init__(**kwargs)
        self.count = count

    def start_jobs(self):
        for item_id in range(self.count):
            yield Task(callback=self.inspect, meta={"item_id": item_id})

    def inspect(self, item_id):
        # Even item IDs pass inspection; odd IDs take the rework branch.
        next_step = self.accept if item_id % 2 == 0 else self.rework
        yield Task(callback=next_step, meta={"item_id": item_id})

    def accept(self, item_id):
        yield Task(
            callback=self.finish, meta={"item_id": item_id, "result": "accepted"}
        )

    def rework(self, item_id):
        yield Task(
            callback=self.finish, meta={"item_id": item_id, "result": "reworked"}
        )

    def finish(self, item_id, result):
        print(f"Item {item_id}: {result}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--count", type=positive_int, default=6, help="Number of items to check."
    )
    parser.add_argument(
        "--redis-key",
        default="example:inspection",
        help="Task namespace for this example.",
    )
    args = parser.parse_args()
    run(InspectionFlow(count=args.count, redis_key=args.redis_key, thread_count=2))


if __name__ == "__main__":
    main()
