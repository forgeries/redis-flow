"""Confirm a batch in Redis before starting workers: python -m examples.submission"""

import argparse

from examples._runner import positive_int, run
from rf import Task, TaskFlow, default


class SubmissionFlow(TaskFlow):
    def receive(self, number):
        print(f"Received item {number}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--count", type=positive_int, default=3, help="Number of tasks."
    )
    parser.add_argument(
        "--redis-key",
        default="example:submission",
        help="Task namespace for this example.",
    )
    args = parser.parse_args()
    flow = SubmissionFlow(
        redis_key=args.redis_key, auto_start_jobs=False, thread_count=2
    )
    batch_size = min(100, default.BUFFER_MAX_RECORDS)
    for offset in range(0, args.count, batch_size):
        flow.submit(
            Task("receive", {"number": number})
            for number in range(offset, min(offset + batch_size, args.count))
        )
    print(f"Redis confirmed {args.count} tasks.")
    run(flow)


if __name__ == "__main__":
    main()
