# Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    subs.add_parser("profiles")
    default_parser = subs.add_parser('defaults', help='Export an editable experiment built from conf/')
    from .catalog import SCALES
    default_parser.add_argument('--scale', choices=SCALES)
    export_parser = subs.add_parser('export')
    export_parser.add_argument('run_id')
    export_parser.add_argument('destination')
    compare_parser = subs.add_parser('compare')
    compare_parser.add_argument('left')
    compare_parser.add_argument('right')
    resolve_parser = subs.add_parser('resolve')
    resolve_parser.add_argument('experiment')
    resolve_parser.add_argument('--profile')
    for name in ("validate", "doctor", "install", "run"):
        sub = subs.add_parser(name)
        sub.add_argument("experiment")
        if name == "run":
            sub.add_argument("--wait", action="store_true")
            sub.add_argument("--request-key")
    for name in ("status", "logs", "cancel", "reconcile"):
        subs.add_parser(name).add_argument("run_id")
    args = parser.parse_args()
    try:
        if args.command == 'defaults':
            print(json.dumps(config.default_experiment(scale=args.scale), indent=2))
        elif args.command == 'profiles':
            from .services.configuration import profiles
            print(json.dumps(profiles(), indent=2))
        elif args.command == 'export':
            from pathlib import Path
            from .services.experiments import export
            Path(args.destination).write_bytes(export(args.run_id))
        elif args.command == 'compare':
            from .services.experiments import compare
            print(json.dumps(compare(args.left, args.right), indent=2))
        elif args.command == 'resolve':
            import yaml
            from pathlib import Path
            from .services.configuration import resolve, get_profile
            base = yaml.safe_load(Path(args.experiment).read_text(encoding='utf-8'))
            profile = get_profile(args.profile)['settings'] if args.profile else None
            print(json.dumps(resolve(base, profile), indent=2))
        elif args.command == "adapters":
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
                if args.command == 'install':
                    from .services.runtime import install
                    print(install(spec['target']))
                else:
                    print(backend.doctor())
            else:
                from .services.experiments import submit
                run_id = submit(spec, args.request_key)['run_id']
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
        elif args.command == "reconcile":
            from .recovery import reconcile
            print(json.dumps(reconcile(args.run_id), indent=2))
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        if getattr(exc, "output", None):
            print(exc.output, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
