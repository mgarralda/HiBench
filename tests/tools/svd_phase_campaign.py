# Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
# SPDX-License-Identifier: Apache-2.0

"""Sequential confirmation campaign using the existing controller services.

python -m tests.tools.svd_phase_campaign experiment.json --output report/svd-confirmation.json
Each scale prepares once, warms infrastructure once per implementation, then
measures ten pairs with alternating order. Every application is a fresh process.
"""
import argparse
import copy
import json
import math
from pathlib import Path
import re
import time
from hibench import config, store
from hibench.services import experiments


def plan(base, scales, pairs):
    jobs = []
    for scale in scales:
        spec = config.validate({**copy.deepcopy(base), 'scale': scale, 'repetitions': 1,
            'workloads': [{'id': 'ml.svd', 'parameters': copy.deepcopy(next(
                (w['parameters'] for w in base['workloads'] if w['id'] == 'ml.svd'), {}))}],
            'target': {**base['target'], 'deploy_mode': 'client'}})
        # Refresh inherited resources for each scale, while preserving explicit overrides.
        original = config.default_experiment(scale=base['scale'])['resources']
        selected = config.default_experiment(scale=scale)['resources']
        spec['resources'] = {key: selected[key] if value == original[key] else value
                             for key, value in spec['resources'].items()}
        jobs.append(('prepare', None, {**spec, 'mode': 'prepare', 'implementation': 'scala'}))
        for language in ('scala', 'pyspark'):
            jobs.append(('warmup', None, {**spec, 'mode': 'run', 'implementation': language}))
        for pair in range(pairs):
            order = ('scala', 'pyspark') if pair % 2 == 0 else ('pyspark', 'scala')
            for language in order:
                jobs.append(('measurement', pair + 1, {**spec, 'mode': 'run', 'implementation': language}))
    return jobs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('experiment', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--scales', nargs='+', choices=('small', 'large'), default=['small', 'large'])
    parser.add_argument('--pairs', type=int, default=10)
    parser.add_argument('--plan-only', action='store_true')
    args = parser.parse_args()
    if args.pairs < 1 or args.pairs > 100:
        parser.error('--pairs must be between 1 and 100')
    if args.output.exists():
        parser.error('Choose a new output file; existing scientific evidence is preserved')
    base = config.resolve(json.loads(args.experiment.read_text(encoding='utf-8')))
    jobs = plan(base, list(dict.fromkeys(args.scales)), args.pairs)
    if args.plan_only:
        print(json.dumps([{'role': role, 'pair': pair, 'configuration': spec} for role, pair, spec in jobs], indent=2))
        return
    if store.has_active_runs():
        raise ValueError('Finish active runs before starting this isolated campaign')
    manifest = {'protocol': 'svd-phases-v1', 'pairs_per_scale': args.pairs,
        'numerical_acceptance': {'rtol': 1e-6, 'atol': 1e-8, 'finite_values': True, 'equal_retained_counts': True},
        'warmup_scope': 'Infrastructure/cache warmup; fresh Spark application for every run. No persistent JVM/Python warmup.',
        'order': 'Alternating Scala-first / PySpark-first pairs', 'runs': [], 'numerical_checks': []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    def save():
        args.output.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    values = {}
    for role, pair, spec in jobs:
        spec['name'] = f"SVD {spec['scale']} {role} {pair or ''} {spec['implementation']}".strip()
        run_id = experiments.submit(spec)['run_id']
        receipt = {'run_id': run_id, 'role': role, 'pair': pair, 'scale': spec['scale'], 'implementation': spec['implementation']}
        manifest['runs'].append(receipt)
        save()
        print(json.dumps(receipt), flush=True)
        while store.get(run_id)['state'] not in ('succeeded', 'failed', 'cancelled'):
            time.sleep(2)
        result = experiments.results(run_id)
        receipt['state'] = result['run']['state']
        receipt['detail'] = result['run']['detail']
        save()
        if receipt['state'] != 'succeeded':
            raise RuntimeError(f"Campaign stopped at {run_id}: {receipt['detail']}")
        if role == 'measurement':
            receipt['metrics'] = result['metrics']
            if len(result['metrics']) != 1 or result['metrics'][0].get('workload_measurement', {}).get('protocol') != 'svd-phases-v1':
                raise RuntimeError('Missing phase instrumentation; update target before confirming performance')
            console = (store.directory(run_id) / 'output.log').read_text(encoding='utf-8', errors='replace')
            singular_values = json.loads(re.search(r'^SVD singularValues=(\[.*\])$', console, re.M)[1])
            key = (spec['scale'], pair)
            if key in values:
                other = values.pop(key)
                if not singular_values or len(other) != len(singular_values):
                    raise RuntimeError('SVD retained-value counts differ')
                error = max(abs(a-b)/max(abs(a), abs(b), 1e-30) for a,b in zip(other, singular_values))
                accepted = all(math.isfinite(a) and math.isfinite(b) and math.isclose(a,b,rel_tol=1e-6,abs_tol=1e-8)
                               for a,b in zip(other, singular_values))
                manifest['numerical_checks'].append({'scale': spec['scale'], 'pair': pair,
                    'retained': len(singular_values), 'max_relative_difference': error, 'accepted': accepted,
                    'scope': 'Singular values only; no reconstruction or eigenvector check'})
                save()
                if not accepted:
                    raise RuntimeError('SVD singular-value agreement failed; inspect preserved evidence before continuing')
            else:
                values[key] = singular_values
            save()
    print('Campaign complete: ' + str(args.output), flush=True)


if __name__ == '__main__':
    main()
