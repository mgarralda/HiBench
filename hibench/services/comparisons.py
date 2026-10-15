# Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
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

"""Analysis snapshots derived from recorded observations, without executing jobs."""
import csv
import copy
import hashlib
import html
import io
import json
import math
import os
import statistics
import time
import uuid
import zipfile
from hibench import store
from . import experiments


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _finite(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: _finite(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_finite(item) for item in value]
    return value


def _observations(result, implementation, workload):
    rows = []
    phase = None
    finished = {(e.get('workload'), e.get('index')) for e in result['events']
                if e.get('kind') == 'phase_finished' and e.get('phase') == 'run'}
    for event in result['events']:
        if event.get('kind') == 'phase_started':
            phase = event
        if event.get('kind') != 'result' or (phase and phase.get('phase') != 'run'):
            continue
        metric = event['metrics']
        language = metric.get('implementation', event.get('implementation', 'scala'))
        identifier = phase.get('workload') if phase else metric.get('workload')
        if implementation and language != implementation or workload and identifier != workload:
            continue
        duration = metric.get('duration_s')
        completed = phase is None or (phase.get('workload'), phase.get('index')) in finished
        valid = completed and isinstance(duration, (int, float)) and not isinstance(duration, bool) and math.isfinite(duration) and duration > 0
        rows.append({**_finite(metric), 'workload': identifier, 'implementation': language,
                     'run_id': result['run']['id'], 'phase_index': phase.get('index') if phase else None,
                     'included': valid, 'exclusion': None if valid else 'Unfinished phase' if not completed else 'Missing or invalid positive duration'})
    return rows


def _summary(rows):
    values = [r['duration_s'] for r in rows if r['included']]
    return {'count': len(values), 'median_s': statistics.median(values) if values else None,
            'min_s': min(values) if values else None, 'max_s': max(values) if values else None,
            'stdev_s': statistics.stdev(values) if len(values) > 1 else None}


def phase_observations(observations):
    """Keep timer protocols separate; absent instrumentation is never zero."""
    rows = []
    for side, samples in observations.items():
        for sample in samples:
            measurement = sample.get('workload_measurement') or {}
            if not sample['included'] or measurement.get('schema_version') != 1 or not measurement.get('protocol'):
                continue
            for phase, seconds in measurement.get('phase_seconds', {}).items():
                if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds < 0:
                    continue
                rows.append({'side': side, 'workload': sample['workload'], 'implementation': sample['implementation'],
                    'run_id': sample['run_id'], 'application_id': sample.get('application_id'),
                    'phase_index': sample.get('phase_index'), 'protocol': measurement['protocol'],
                    'phase': phase, 'duration_s': seconds,
                    'numerical_runtime': json.dumps(measurement.get('numerical_runtime', {}), sort_keys=True)})
    return rows


def phase_summary(observations):
    samples = phase_observations(observations)
    rows = []
    for workload, protocol, phase in sorted({(r['workload'], r['protocol'], r['phase']) for r in samples}):
        a, b = [_summary([{**r, 'included': True} for r in samples
                         if (r['workload'], r['protocol'], r['phase'], r['side']) == (workload, protocol, phase, side)])
                for side in ('left', 'right')]
        ratio = b['median_s'] / a['median_s'] if a['median_s'] and b['median_s'] is not None else None
        rows.append({'workload': workload, 'protocol': protocol, 'phase': phase, 'left': a, 'right': b,
                     'ratio': ratio, 'difference_pct': (ratio-1)*100 if ratio is not None else None})
    return rows


def analyze(left, right, left_implementation=None, right_implementation=None, workload=None):
    for language in (left_implementation, right_implementation):
        if language not in (None, 'scala', 'pyspark'):
            raise ValueError('Select scala or pyspark')
    sources = [experiments.results(left), experiments.results(right)]
    if any(s['run']['state'] not in ('succeeded', 'failed', 'cancelled') for s in sources):
        raise ValueError('Compare completed runs; active results are still changing')
    a = _observations(sources[0], left_implementation, workload)
    b = _observations(sources[1], right_implementation, workload)
    if any(len({r['implementation'] for r in rows}) > 1 for rows in (a, b)):
        raise ValueError('Select one implementation on each side; languages must not be pooled')
    conditions = []

    def condition(field, av, bv):
        status = 'Unknown' if av is None or bv is None else 'Same' if av == bv else 'Different'
        conditions.append({'field': field, 'left': av, 'right': bv, 'status': status})

    for field in ('scale', 'resources', 'target', 'monitoring'):
        condition(field, *(s['run']['spec'].get(field) for s in sources))
    for field in ('resolved_spark_properties', 'effective_workload_parameters'):
        condition(field, *(s['provenance'].get(field) for s in sources))
    condition('implementation', next(iter({r['implementation'] for r in a}), left_implementation),
              next(iter({r['implementation'] for r in b}), right_implementation))
    condition('effective_deploy_mode', sorted({r['deploy_mode'] for r in a if r.get('deploy_mode')}) or None,
              sorted({r['deploy_mode'] for r in b if r.get('deploy_mode')}) or None)
    def datasets(source):
        return {e.get('workload', e.get('manifest', {}).get('workload', {}).get('id')): e['manifest'].get('key')
                for e in source['events'] if e.get('kind') in ('dataset_reused', 'dataset_prepared') and 'manifest' in e}
    condition('dataset_manifest_keys', datasets(sources[0]) or None, datasets(sources[1]) or None)
    condition('runtime_versions', None, None)
    condition('source_file_sha256', *(s['provenance'].get('file_sha256') for s in sources))
    summaries = []
    for identifier in sorted({r['workload'] for r in a + b if r['workload']}):
        av, bv = (_summary([r for r in rows if r['workload'] == identifier]) for rows in (a, b))
        ratio = bv['median_s'] / av['median_s'] if av['median_s'] and bv['median_s'] else None
        summaries.append({'workload': identifier, 'left': av, 'right': bv, 'ratio': ratio,
                          'difference_pct': (ratio - 1) * 100 if ratio is not None else None})
    return {'schema_version': 1, 'analysis_version': 2, 'created_at': time.time(),
            'selection': {'left': left, 'right': right, 'left_implementation': left_implementation,
                          'right_implementation': right_implementation, 'workload': workload},
            'source_sha256': [_digest(s) for s in sources], 'conditions': conditions,
            'source_states': [s['run']['state'] for s in sources],
            'source_errors': [[e for e in s['events'] if e.get('kind') == 'error'] for s in sources],
            'observations': {'left': a, 'right': b}, 'summary': summaries,
            'phase_summary': phase_summary({'left': a, 'right': b}),
            'scope': 'Duration includes submission and Spark startup. Ratios are B / A. Dataset keys describe preparation conditions, not content checksums. Runtime versions and output equivalence are not certified. Missing metrics and unfinished phases are excluded; completed observations from failed runs remain visible. No causal or statistical equivalence claim.'}


def save(analysis):
    identifier = str(uuid.uuid4())
    directory = store.home() / 'comparisons' / identifier
    directory.mkdir(parents=True)
    result = {**copy.deepcopy(analysis), 'comparison_id': identifier}
    temporary = directory / 'comparison.tmp'
    temporary.write_text(json.dumps(result, indent=2, allow_nan=False), encoding='utf-8')
    os.replace(temporary, directory / 'comparison.json')
    return result


def load(identifier):
    uuid.UUID(identifier)
    path = store.home() / 'comparisons' / identifier / 'comparison.json'
    if not path.is_file():
        raise ValueError('Unknown comparison')
    return json.loads(path.read_text(encoding='utf-8'))


def recent():
    root = store.home() / 'comparisons'
    return sorted([{'comparison_id': p.parent.name, 'created_at': data['created_at'], 'selection': data['selection']}
                   for p in root.glob('*/comparison.json') for data in [json.loads(p.read_text(encoding='utf-8'))]],
                  key=lambda x: x['created_at'], reverse=True)


def source_status(analysis):
    statuses = []
    for index, side in enumerate(('left', 'right')):
        try:
            current = experiments.results(analysis['selection'][side])
            statuses.append('Unchanged' if _digest(current) == analysis['source_sha256'][index] else 'Changed')
        except (ValueError, OSError):
            statuses.append('Unavailable')
    return statuses


def _csv(rows):
    stream = io.StringIO(newline='')
    keys = list(dict.fromkeys(key for row in rows for key in row))
    writer = csv.DictWriter(stream, fieldnames=keys)
    writer.writeheader()
    for row in rows:
        # Spreadsheet-safe text, including user-supplied workload/application identifiers.
        writer.writerow({key: "'" + value if isinstance(value, str) and value.startswith(('=', '+', '-', '@')) else value
                         for key, value in row.items()})
    return stream.getvalue()


def artifacts(analysis):
    observations = [{**row, 'side': side} for side, rows in analysis['observations'].items() for row in rows]
    summaries = [{'workload': r['workload'], 'ratio': r['ratio'], 'difference_pct': r['difference_pct'],
                  **{side + '_' + k: v for side in ('left', 'right') for k, v in r[side].items()}}
                 for r in analysis['summary']]
    def table(rows):
        if not rows:
            return '<p>No observations available.</p>'
        keys = list(rows[0])
        return '<table><tr>' + ''.join('<th>' + html.escape(k) + '</th>' for k in keys) + '</tr>' + ''.join(
            '<tr>' + ''.join('<td>' + html.escape(str(row.get(k, ''))) + '</td>' for k in keys) + '</tr>' for row in rows) + '</table>'
    # Embedded SVG: independent of a plotting server, JavaScript or network access.
    svg = '<svg xmlns="http://www.w3.org/2000/svg" width="640" height="%s" role="img" aria-label="Duration ratio B over A">' % (60 + 35 * len(summaries))
    maximum = max([r['ratio'] for r in summaries if r['ratio'] is not None] + [1]) * 1.1
    reference = 220 + 350 / maximum
    svg += f'<line x1="{reference}" x2="{reference}" y1="0" y2="{60 + 35 * len(summaries)}" stroke="gray"/><text x="{reference}" y="15">1×</text>'
    for index, row in enumerate(summaries):
        y = 35 + index * 35
        svg += f'<text x="5" y="{y + 14}">{html.escape(row["workload"])}</text>'
        if row['ratio'] is not None:
            svg += f'<rect x="220" y="{y}" width="{350 * row["ratio"] / maximum}" height="20" fill="#3478ba"/><text x="580" y="{y + 14}">{row["ratio"]:.3f}×</text>'
    svg += '</svg>'
    points = '<svg xmlns="http://www.w3.org/2000/svg" width="640" height="%s" role="img" aria-label="Individual durations and medians">' % (50 + 60 * len(summaries))
    for index, row in enumerate(analysis['summary']):
        group = [r for r in observations if r['workload'] == row['workload'] and r['included']]
        maximum_duration = max([r['duration_s'] for r in group] + [1]) * 1.1
        y = 30 + index * 60
        points += f'<text x="5" y="{y}">{html.escape(row["workload"])}</text>'
        for offset, side, color in ((0, 'left', '#3478ba'), (22, 'right', '#d47c24')):
            points += f'<text x="185" y="{y + offset}">{"A" if side == "left" else "B"}</text>'
            for observation in group:
                if observation['side'] == side:
                    x = 220 + 350 * observation['duration_s'] / maximum_duration
                    points += f'<circle cx="{x}" cy="{y + offset - 4}" r="4" fill="{color}"><title>{observation["duration_s"]:.3f} s</title></circle>'
            median = row[side]['median_s']
            if median is not None:
                x = 220 + 350 * median / maximum_duration
                points += f'<line x1="{x}" x2="{x}" y1="{y + offset - 12}" y2="{y + offset + 4}" stroke="black"><title>Median {median:.3f} s</title></line>'
    points += '</svg>'
    report = '<!doctype html><html lang="en"><meta charset="utf-8"><title>HiBench Next comparison</title><style>body{font:14px sans-serif;margin:32px}table{border-collapse:collapse;margin:20px 0}td,th{border:1px solid #ccc;padding:8px;text-align:left}svg{max-width:100%}</style><h1>HiBench Next comparison</h1><p>' + html.escape(analysis['scope']) + '</p><h2>Selection and source evidence</h2><pre>' + html.escape(json.dumps({key: analysis[key] for key in ('selection', 'created_at', 'source_sha256', 'source_states', 'source_errors')}, indent=2)) + '</pre><h2>Conditions</h2>' + table(analysis['conditions']) + '<h2>Duration ratios B / A</h2>' + svg + table(summaries) + '<h2>Individual durations (s) and medians</h2>' + points + '<h2>Observations</h2>' + table(observations) + '</html>'
    phases = [{'workload': r['workload'], 'protocol': r['protocol'], 'phase': r['phase'], 'ratio': r['ratio'],
               **{side + '_' + key: value for side in ('left', 'right') for key, value in r[side].items()}}
              for r in analysis.get('phase_summary', [])]
    if phases:
        report = report.replace('</html>', '<h2>Instrumented phase durations</h2><p>Timers follow the named protocol. Nested totals are not additive; missing measurements are not zero. These are not Spark SQL durations.</p>' + table(phases) + '</html>')
    files = {'comparison.json': json.dumps(analysis, indent=2, allow_nan=False),
            'observations.csv': _csv(observations), 'summary.csv': _csv(summaries), 'report.html': report}
    if phases:
        files.update({'phases.csv': _csv(phase_observations(analysis['observations'])), 'phase-summary.csv': _csv(phases)})
    return files


def export(analysis):
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as bundle:
        for name, content in artifacts(analysis).items():
            bundle.writestr(name, content)
    return output.getvalue()
