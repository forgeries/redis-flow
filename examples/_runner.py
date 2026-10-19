"""Share interrupt handling and graceful shutdown across runnable examples."""

import argparse


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("Enter a whole number greater than zero.")
    return number


def run(flow):
    flow.start()
    try:
        # Poll joins so Ctrl+C stays responsive on Windows and Linux.
        while flow.is_alive():
            flow.join(0.2)
    except KeyboardInterrupt:
        print("Stop requested. Wait for active tasks and buffered writes to finish.")
        return False
    finally:
        flow.stop()
        while flow.is_alive():
            flow.join(0.2)
    return True
