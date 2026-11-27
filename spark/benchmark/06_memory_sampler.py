import argparse
import csv
import time
from pathlib import Path


CGROUP = Path("/sys/fs/cgroup")


def read_memory():
    current = int(
        (CGROUP / "memory.current").read_text().strip()
    )

    stats = {}

    for line in (CGROUP / "memory.stat").read_text().splitlines():
        key, value = line.split()
        stats[key] = int(value)

    inactive_file = stats.get("inactive_file", 0)

    return {
        "memory_current_bytes": current,
        "working_set_bytes": max(0, current - inactive_file),
        "anonymous_bytes": stats.get("anon", 0),
        "file_cache_bytes": stats.get("file", 0),
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--output", required=True)
    parser.add_argument("--stop-file", required=True)
    parser.add_argument("--interval", type=float, default=0.1)
    parser.add_argument("--max-seconds", type=float, default=600)

    args = parser.parse_args()

    if args.interval <= 0 or args.max_seconds <= 0:
        raise ValueError("Invalid sampling parameters")

    output = Path(args.output)
    stop_file = Path(args.stop_file)

    output.parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "timestamp",
        "memory_current_bytes",
        "working_set_bytes",
        "anonymous_bytes",
        "file_cache_bytes",
    ]

    start = time.monotonic()
    deadline = start

    with output.open("w", newline="") as file:

        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()

        while not stop_file.exists():

            if time.monotonic() - start >= args.max_seconds:
                break

            writer.writerow({
                "timestamp": time.time(),
                **read_memory()
            })

            file.flush()

            deadline += args.interval

            delay = deadline - time.monotonic()

            if delay > 0:
                time.sleep(delay)
            else:
                deadline = time.monotonic()

    print("Memory sampling completed")


if __name__ == "__main__":
    main()