import argparse
import json
import sys
import time
from . import config, store
from .adapters import create_adapter, REGISTRY
from .catalog import workloads
from .worker import launch


def main():
    parser = argparse.ArgumentParser(description="HiBench batch experiments")
    subs = parser.add_subparsers(dest="command", required=True)
    subs.add_parser("list")
    subs.add_parser("adapters")
    for name in ("validate", "doctor", "install", "run"):
        sub = subs.add_parser(name)
        sub.add_argument("experiment")
        if name == "run":
            sub.add_argument("--wait", action="store_true")
            sub.add_argument("--request-key")
    for name in ("status", "logs", "cancel"):
        subs.add_parser(name).add_argument("run_id")
    args = parser.parse_args()
    try:
        if args.command == "adapters":
            from dataclasses import asdict
            print(json.dumps([asdict(entry) for entry in REGISTRY], indent=2))
        elif args.command == "list":
            print(json.dumps(workloads(), indent=2))
        elif args.command in ("validate", "doctor", "install", "run"):
            spec = config.load(args.experiment)
            if args.command == "validate":
                print(json.dumps(spec, indent=2))
            elif args.command in ("doctor", "install"):
                backend = create_adapter(spec["target"])
                print(getattr(backend, args.command)())
            else:
                run_id, created = store.create(spec, args.request_key)
                if created:
                    launch(run_id)
                print(run_id, flush=True)
                if args.wait:
                    while True:
                        row = store.get(run_id)
                        if row["state"] in ("succeeded", "failed", "cancelled"):
                            print(json.dumps(row, indent=2))
                            return 0 if row["state"] == "succeeded" else 1
                        time.sleep(1)
        elif args.command == "status":
            print(json.dumps(store.get(args.run_id), indent=2))
        elif args.command == "logs":
            path = store.directory(args.run_id) / "output.log"
            print(path.read_text(encoding="utf-8") if path.exists() else "No output yet")
        elif args.command == "cancel":
            store.cancel(args.run_id)
            print("Cancellation requested")
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        if getattr(exc, "output", None):
            print(exc.output, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
