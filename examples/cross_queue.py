"""Transfer tasks between queues: python -m examples.cross_queue --count 4"""

import argparse

from examples._runner import positive_int, run
from rf import BaseProcess, Queue, Task, TaskFlow, default


class Producer(BaseProcess):
    def __init__(self, destination, count):
        self.destination = destination
        self.count = count

    def start_jobs(self):
        for order_id in range(self.count):
            yield Task(callback=self.dispatch, meta={"order_id": order_id})

    def dispatch(self, order_id):
        # Queue takes a full Redis key, while TaskFlow takes a namespace.
        yield Queue(
            redis_key=default.TAB_JOB.format(redis_key=self.destination),
            task=Task(
                process_name="Consumer", callback="receive", meta={"order_id": order_id}
            ),
        )


class Consumer(BaseProcess):
    def receive(self, order_id):
        print(f"Received order {order_id}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--count", type=positive_int, default=4, help="Number of orders to send."
    )
    parser.add_argument(
        "--redis-key",
        default="example:delivery",
        help="Shared task namespace for both queues.",
    )
    parser.add_argument(
        "--role",
        choices=["all", "producer", "consumer"],
        default="all",
        help=(
            "Use all to send, receive, and exit. Use producer to send and exit. "
            "Use consumer to wait for tasks. Press Ctrl+C to stop."
        ),
    )
    args = parser.parse_args()
    destination = args.redis_key + ":consumer"

    if args.role in ("all", "producer"):
        producer = TaskFlow(redis_key=args.redis_key + ":producer", thread_count=2)
        producer.add_process(Producer, destination=destination, count=args.count)
        if not run(producer):
            return

    if args.role in ("all", "consumer"):
        consumer = TaskFlow(
            redis_key=destination,
            thread_count=2,
            auto_start_jobs=False,
            keep_alive=args.role == "consumer",
        )
        consumer.add_process(Consumer)
        run(consumer)


if __name__ == "__main__":
    main()
