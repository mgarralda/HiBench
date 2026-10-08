# HiBench Next modernization evidence

This directory is the entry point for the final engineering report and a possible research-software article. It records completed work, reference provenance, validation limits and future work. It is not a claim that the entire suite or every managed platform is already supported.

## Records

- [Migration ledger](migrations.json): workload-level status, data contracts, algorithm changes and evidence links. Completed entries must retain their historical evidence; add a new version when behavior changes.
- [Reporting protocol](reporting-protocol.md): required information for every subsequent generator and workload migration.
- [Publication and API roadmap](publication-roadmap.md): intended scientific contribution and the role of UI, programmatic access and the companion cluster.
- [Evidence inventory](evidence-manifest.json): SHA-256 digests of reference fixtures, validation reports, configurations, verifier sources and selected durable logs.
- `evidence/`: copied validation logs. These copies survive cleanup of the ignored `report/` directory.

Existing dated audit reports are historical baselines, not the current capability catalog. In particular, references to removed DAL, Mahout readers, Hive SQL and RDD micro implementations describe earlier states. Current contracts are in [micro](../micro-modernization.md), [SQL](../sql-modernization.md) and [ML](../ml-modernization.md) modernization notes.

## Recovery and integrity

Run `python bin/update_evidence_manifest.py` after updating the ledger and copying
validation logs. It checks referenced files and refreshes their SHA-256 inventory.

Run `python bin/reproducibility_snapshot.py` from the repository to create an immutable timestamped local snapshot under `report/reproducibility/`. Each snapshot includes current source files selected by Git, a binary Git diff, changed/deleted-file status, source hashes, baseline revision and available runtime JAR hashes. Git metadata, ignored files, credentials directories, environments, build trees and benchmark datasets are excluded. Selected evidence must first be copied into this directory to be included.

To recover a source snapshot, extract its `source/` contents into a new empty directory and verify every path against `snapshot-manifest.json`. To reconstruct the corresponding Git working tree, check out its recorded baseline revision in a separate checkout, apply `working-tree.patch`, then overlay the source snapshot, removing paths listed as deleted. The archive is a source recovery artifact; it does not include live HDFS data, Docker volumes, credentials or external environments. Rebuild using the documented runtime and rerun the versioned experiment configurations.

The archive hash is stored alongside it in `snapshot.sha256`. Retain archives externally before cleaning `report/`; they are local backups, not a replacement for committing and publishing a reviewed release. The script does not commit, push or change the working tree. Keep journal/release artifacts tied to an eventual immutable release commit and archive DOI.

## Recorded environment

Validation runtime: Spark 3.5.9, Scala 2.12.18, Java 11 and Hadoop 3.3.6 on the Docker/YARN companion cluster. The companion repository was clean at revision `379c801b92d3baf6b2c9e70532e21825107861a0` when recorded on 2026-10-08. This revision is context for the tested environment, not a claim that other revisions or platforms are certified.
