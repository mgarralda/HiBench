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

"""Consolidate recorded evidence; never infer fresh cluster certification."""
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from hibench.catalog import IDS, read_properties, workloads


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def observations(value, pointer=""):
    """Keep JSON pointers so shared receipt observations remain attributable."""
    found = []
    if isinstance(value, dict):
        for key, child in value.items():
            location = pointer + "/" + key.replace("~", "~0").replace("/", "~1")
            if key in ("spec", "state", "run_id", "application_ids", "algorithm_version",
                       "algorithm_versions", "data_contract", "contract", "scale",
                       "seed", "shuffle_partitions", "generation_partitions"):
                found.append({"pointer": location, "value": child})
            found.extend(observations(child, location))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(observations(child, pointer + "/" + str(index)))
    return found


def apply_intermediate_evidence(entry, measurements, checks):
    name = entry['workload']
    timing = next((r for r in measurements.get('workloads', []) if r['workload'] == name), None)
    check = next((r for r in checks if r['workload'] == name), None)
    if timing and timing.get('complete') and check and check.get('status') == 'passed':
        entry['representative_performance_campaign'] = 'intermediate-v1 local observations recorded; large/external pending'
        entry['intermediate_timing'] = timing
        entry['intermediate_structural_validation'] = check
        entry['intermediate_receipt'] = 'docs/reproducibility/evidence/2026-10-09/intermediate-v1/measurements.json'
        entry['gaps'] = ['Large repeated campaigns and external reproduction.',
                         'Independent numerical acceptance remains workload-specific; local timings are not scalability certification.']


def apply_preset_evidence(entry, campaign, checks, references, measurements):
    name = entry['workload']
    run = next((r for r in campaign.get('runs', []) if r['workload'] == name), None)
    check = next((r for r in checks if r['workload'] == name), None)
    timing = next((r for r in measurements.get('workloads', []) if r['workload'] == name), None)
    if not run or run['state'] != 'succeeded' or run['spec']['scale'] != 'tiny':
        return
    if any(w['parameters'] for w in run['spec']['workloads']):
        return
    actual = next((r for r in references if r['case'] == 'actual'), {})
    if not actual.get('checks_passed') or not timing or not timing.get('complete') or not check:
        return
    passed = check.get('status') == 'passed'
    if name == 'graph.nweight':
        passed = check.get('status') == 'dedicated_verifier_required' and any(
            r['case'] == 'nweight' and r.get('checks_passed') for r in references)
    if passed:
        entry['default_preset_acceptance'] = {
            'tiny': {'status': 'functional checks recorded; numerical fit scope is workload-specific',
                     'run_id': run['run_id'], 'parameters_overridden': False,
                     'declared_defaults': run['default_parameters'], 'input_output_checks': check,
                     'receipt': 'docs/reproducibility/evidence/2026-10-09/default-preset-tiny/campaign.json'}}


def build(root=ROOT):
    ledger = json.loads((root / "docs/reproducibility/migrations.json").read_text(encoding="utf-8"))
    records = {r["workload"]: r for r in ledger["records"] if r["workload"] in IDS}
    if set(records) != set(IDS):
        raise ValueError("Migration ledger must cover exactly the maintained catalog")
    catalog = {item["id"]: item for item in workloads(root)}
    candidate_path = ROOT / 'docs/reproducibility/candidate-acceptance-2026-10-09.json'
    candidate = json.loads(candidate_path.read_text(encoding='utf-8')) if candidate_path.exists() else {}
    candidate_runs = {r['workload']: r for r in candidate.get('runs', [])}
    intermediate_dir = ROOT / 'docs/reproducibility/evidence/2026-10-09/intermediate-v1'
    measurement_path = intermediate_dir / 'measurements.json'
    checks_path = intermediate_dir / 'structural-results.json'
    measurements = json.loads(measurement_path.read_text()) if measurement_path.exists() else {}
    intermediate_checks = json.loads(checks_path.read_text()) if checks_path.exists() else []
    preset_dir = ROOT / 'docs/reproducibility/evidence/2026-10-09/default-preset-tiny'
    def preset_json(name, default):
        path = preset_dir / name
        return json.loads(path.read_text()) if path.exists() else default
    preset_campaign = preset_json('campaign.json', {})
    preset_checks = preset_json('actual-results.json', [])
    preset_references = preset_json('verification-receipt.json', [])
    preset_measurements = preset_json('measurements.json', {})
    receipts, entries = {}, []
    for name in IDS:
        record = records[name]
        evidence = record["validation"]
        if evidence not in receipts:
            path = root / evidence
            receipt = json.loads(path.read_text(encoding="utf-8"))
            receipts[evidence] = {
                "sha256": digest(path), "date": receipt.get("date"),
                "observations": observations(receipt),
                "receipt_scope": "May include multiple workloads; pointers are not per-workload certification.",
            }
        category, workload = name.split(".")
        config_path = "conf/workloads/" + category + "/" + workload + ".conf"
        paths = {record["documentation"], evidence, config_path}
        for field in ("scale_or_profile_reference", "verifiers", "implementation"):
            paths.update(record.get(field, []))
        hashes = {}
        for path_name in sorted(paths):
            path = (root / path_name).resolve()
            if root.resolve() not in path.parents or not path.is_file():
                raise ValueError("Missing or unsafe acceptance evidence: " + path_name)
            hashes[path_name] = digest(path)
        entries.append({
            "workload": name,
            "migration_status": record["status"],
            "acceptance_status": "partial",
            "historical_evidence": "Recorded functional/profile validation; see receipt and contract.",
            "current_revision_cluster_acceptance": "pending",
            "representative_performance_campaign": "pending",
            "external_reproduction": "pending",
            "execution_available": catalog[name].get("execution_available"),
            "requires_classic": catalog[name].get("requires_classic"),
            "contract": record.get("contract"),
            "generator_changes": record.get("generator_changes"),
            "workload_changes": record.get("workload_changes"),
            "configuration": {"path": config_path,
                              "current_properties": read_properties(root / config_path),
                              "parameter_schema": catalog[name]["parameters"]},
            "documentation": record["documentation"],
            "receipt": evidence,
            "limits": record.get("limits", []),
            "evidence_sha256": hashes,
            "gaps": [
                "Consolidated prepare/run/output acceptance on a frozen candidate revision.",
                "Effective runtime Spark properties and separated measurement intervals.",
                "Feasible intermediate/large repeated campaigns and external reproduction.",
            ],
        })
        if name in candidate_runs:
            run = candidate_runs[name]
            entries[-1]['candidate_receipt'] = 'docs/reproducibility/candidate-acceptance-2026-10-09.json'
            entries[-1]['candidate_run_id'] = run['run_id']
            entries[-1]['candidate_execution_state'] = run['state']
            entries[-1]['current_revision_cluster_acceptance'] = 'execution_recorded; independent output acceptance pending'
            entries[-1]['candidate_result_validation'] = run.get('independent_validation', 'pending')
            if isinstance(entries[-1]['candidate_result_validation'], dict) and entries[-1]['candidate_result_validation'].get('status') == 'passed':
                entries[-1]['current_revision_cluster_acceptance'] = 'bounded checks recorded; performance/external pending'
                entries[-1]['gaps'] = [
                    'Effective runtime Spark properties and separated measurement intervals.',
                    'Feasible intermediate/large repeated campaigns and external reproduction.',
                    'Independent ML fit numerical acceptance is workload-specific; consult check scope.',
                ]
        apply_intermediate_evidence(entries[-1], measurements, intermediate_checks)
        apply_preset_evidence(entries[-1], preset_campaign, preset_checks, preset_references, preset_measurements)
    return {"schema_version": 1, "evidence_source": str(root.resolve()),
            "link_prefix": '' if root.resolve() == ROOT else root.resolve().relative_to(ROOT).as_posix() + '/',
            "scope": "Explicit source evidence inventory, not release certification.",
            "method": "No status inferred from shared success flags or catalog availability.",
            "workload_count": len(entries), "receipts": receipts, "workloads": entries}


def render(data):
    lines = ["# Matriz de aceptación funcional de HiBench Next", "",
             "Inventario reproducible de los 24 workloads; no certificación de release.", "",
             "**Estado:** migración registrada en todas las cargas. Si existe recibo de candidata se distingue ejecución registrada de verificación independiente de resultados; la aceptación completa sigue parcial.", "",
             "El [JSON](acceptance-matrix.json) contiene propiedades actuales, esquemas de parámetros, contratos, hashes y punteros a configuraciones/estados de los recibos. Los recibos compartidos se conservan como compartidos: un estado `succeeded` no acredita por sí solo cada carga ni sus resultados. Las propiedades actuales no sustituyen la configuración histórica ejecutada.", "",
             "| Workload | Migración | Aceptación candidata | Preset tiny | Campaña intermedia | Contrato y recibo histórico |",
             "| --- | --- | --- | --- | --- | --- |"]
    for item in data["workloads"]:
        doc = "../../" + data['link_prefix'] + item["documentation"]
        receipt = "../../" + data['link_prefix'] + item["receipt"]
        validation = item.get('candidate_result_validation')
        checked = isinstance(validation, dict) and validation.get('status') == 'passed'
        status = ('Ejecución y checks acotados acreditados; consultar alcance' if checked else
                  'Ejecución ' + item['candidate_execution_state'] + '; resultados pendientes') if 'candidate_execution_state' in item else 'Parcial; cierre pendiente'
        intermediate = '[Tiempos y checks](intermediate-campaign-2026-10-09.md)' if item.get('intermediate_timing') else 'Pendiente'
        preset = '[Checks acreditados](default-preset-validation-2026-10-09.md)' if item.get('default_preset_acceptance') else 'Pendiente'
        lines.append(f"| `{item['workload']}` | {item['migration_status']} | {status} | {preset} | {intermediate} | [Contrato]({doc}) · [Recibo]({receipt}) |")
    lines.extend(["", "## Criterios de cierre", "",
                  "1. Fijar revisión, artefactos y entorno; resolver parámetros, semilla, particiones y propiedades efectivas por ejecución.",
                  "2. Registrar prepare/run, IDs y estados remotos y validar volumen/perfil y resultado contra la referencia correspondiente. Sleep y los modos en memoria requieren su contrato específico.",
                  "3. Acreditar el resultado de cada carga; conservar fallos y correcciones, sin reinterpretar antiguos recibos como pruebas nuevas.",
                  "4. Separar preparación, envío/arranque y ejecución; campañas de rendimiento y reproducción externa son niveles distintos.", "",
                  "## Huecos comunes", "",
                  "- Campaña candidata: consultar [recibo](candidate-acceptance-2026-10-09.json) cuando esté disponible; ejecución y resultados se acreditan separadamente.",
                  "- Cancelación y reconciliación explícita: consultar el recibo de candidata para las pruebas reales; recuperación automática no implementada.",
                  "- Propiedades efectivas, concordancia completa UI/CLI y controles multiusuario: no se acreditan por un smoke run ni por mocks.",
                  "- Escalas intermedias/grandes: seleccionar por filas/dimensiones/aristas y presupuesto; no asumir que todos los presets son factibles (Correlation/SVD).",
                  "- Cloud/serverless y reproducción externa: no certificados.", "",
                  "Regenerar desde la raíz: `.venv/Scripts/python.exe tests/tools/build_acceptance_matrix.py`. Los hashes identifican evidencia actual; no prueban que ese contenido se ejecutase en un recibo anterior.", ""])
    return "\n".join(lines)


if __name__ == "__main__":
    matrix = build(Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else ROOT)
    destination = ROOT / "docs/reproducibility"
    (destination / "acceptance-matrix.json").write_text(json.dumps(matrix, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    (destination / "acceptance-matrix.md").write_text(render(matrix), encoding="utf-8")
    print(f"Inventoried {matrix['workload_count']} workloads and {len(matrix['receipts'])} receipts")
