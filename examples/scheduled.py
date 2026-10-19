"""Schedule a daily flow with --at HH:MM, or run immediately with --once."""

import argparse
from datetime import datetime

from examples._runner import run
from examples.quickstart import HelloFlow


def daily_time(value):
    try:
        return datetime.strptime(value, "%H:%M").strftime("%H:%M")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Use HH:MM, for example 20:30.") from exc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--at",
        type=daily_time,
        help="Run once each day at HH:MM. Use local system time.",
    )
    mode.add_argument("--once", action="store_true", help="Run once now. Then exit.")
    parser.add_argument(
        "--redis-key",
        default="example:scheduled",
        help="Task namespace for this example.",
    )
    args = parser.parse_args()
    if args.at:
        print(
            f"Run each day at {args.at} (local system time). Press Ctrl+C to stop.",
            flush=True,
        )
    run(HelloFlow(redis_key=args.redis_key, cron_hour=args.at, thread_count=2))


if __name__ == "__main__":
    main()
