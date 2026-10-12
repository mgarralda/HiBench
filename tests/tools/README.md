# Scientific validation tools

These tools support development and scientific evidence collection. They are
not imported by the HiBench UI, API, CLI or workload runtime and are excluded
from the installed `hibench` package and deployment image.

Run from the repository root with the development environment:

For a new current-default campaign, build the 2.0.0 runtime (or use the Compose
image), then generate and review an experiment:

```sh
python -m hibench.cli defaults --scale tiny > experiment.json
python -m hibench.cli validate experiment.json
python -m hibench.cli install experiment.json
python -m hibench.cli doctor experiment.json
python tests/tools/run_preset_campaign.py experiment.json report/preset-tiny-current
python -m tests.tools.scale_verification report/preset-tiny-current
```

Review workload selection before submitting: the experiment follows the current
`conf/benchmarks.lst`. Workload parameters must remain empty for named-preset
acceptance. Preparation/execution success still needs applicable output checks.
Bundled `tests/fixtures/experiments/` recipes preserve explicit validation resource
choices; they do not automatically inherit today's canonical resource defaults.
Dated reports retain former paths and runtime hashes. They are historical
records, not current commands; exact reproduction needs the recorded snapshot.

- `python tests/tools/run_preset_campaign.py EXPERIMENT DESTINATION` validates
  unmodified named sizes using recipes in `tests/fixtures/experiments/`.
- `python -m tests.tools.scale_verification DESTINATION` verifies generated
  inputs and outputs of a completed campaign.
- `python -m tests.tools.campaign_measurement DESTINATION` collects Spark event
  log measurements.
- `python tests/tools/reproducibility_snapshot.py` archives current source.
- `python tests/tools/build_acceptance_matrix.py` consolidates recorded evidence.
- `python tests/tools/update_evidence_manifest.py` updates evidence hashes.

Reusable bounded candidate acceptance, resilience checks and intermediate campaigns remain
optional development tools; inspect their profiles before use. They are not release
certification for a new revision. The three date-bound preparation/recording/follow-up
scripts were retired into the verified archive
`docs_internal/test-tools-retired-2026-10-11.zip` with a SHA-256 inventory.
Historical receipts retain original paths and hashes; they are not rewritten.
