# General configuration review — 2026-10-08

Execution adapters and storage must be independent: an execution platform may use HDFS or object storage. The general UI should describe storage rather than call every location an HDFS path.

## Delivered configuration controls

**Parametrization → General** now edits HDFS endpoint, dataset/output base URIs, Hadoop configuration directory, remote report directory, Spark event-log enablement/URI and YARN History Server address for the current experiment. Existing YAML files retain the previous defaults. Changes are included in experiment exports; saved runs are unchanged.

Custom data/output directories must be non-root paths on the configured HDFS endpoint. Per-dataset and per-run isolation remains in place, and target settings are included in dataset fingerprints. HA nameservice endpoints are accepted syntactically, but require an appropriate runtime Hadoop configuration. Default-filesystem-only URIs remain unsupported.

Doctor explicitly checks the saved HDFS endpoint and respects the selected Hadoop configuration directory. Dataset availability, YARN management and submitted scripts use that configuration too. Provider options for S3, ADLS and Cloud Storage are visible as soon and cannot be applied. Automatic reuse and existing resource controls remain unchanged.

Validation: 35 Python tests pass, including General UI persistence, unsupported-provider blocking, directory validation, dataset identity and staged runtime propagation. Real Docker/HDFS/YARN doctor passes. No benchmark was launched and no distributed data was moved by this change.

## Review baseline

| Area | Current behavior | Proposed control |
|---|---|---|
| Storage endpoint | `target.hdfs_uri`, editable in Experiments; classic adapters require `hdfs://host:port` | Storage backend/provider and validated endpoint |
| Prepared data | `<hdfs_uri>/HiBench-Control/datasets/<fingerprint>-<suffix>` | Configurable dataset base URI, retaining generated isolated subdirectories |
| Benchmark output | `<hdfs_uri>/HiBench-Control/runs/<run>/<workload>/<index>` | Configurable output base URI, retaining per-run isolation |
| Remote reports | `<workspace>/report/control/<run>/<workload>/<index>` | Report location or profile default |
| Controller history | Local SQLite, manifests and logs under `report/control`, or `HIBENCH_STATE_DIR` | Application setting; separate from distributed benchmark data |
| Spark event logs | Defaults inherited from `conf/spark.conf`, overridable through `spark_conf` | Enable logs, event-log URI, history-server address |
| Runtime configuration | Hadoop config assumed to be `<hadoop_home>/etc/hadoop` | Explicit Hadoop configuration directory |
| Experiment resources | Executors, executor cores/memory, driver memory, generation/shuffle partitions already editable | Retain existing resource controls; expose relevant driver/task settings |
| Remaining MapReduce generators | Map/reduce memory hardcoded to 1024 MB by the controller | Advanced preparation resource settings |
| Dataset reuse | Successful matching manifest is reused automatically, including prepare-and-run | Explicit reuse versus regenerate policy |
| Spark behavior | Additional Spark YAML supports AQE, dynamic allocation and other properties | Common typed controls plus advanced configuration |

The standalone `hibench.hdfs.data.dir` in `conf/hibench.conf` does not control controller-run dataset roots: per-phase overrides replace it. Input and output formats also have workload-specific contracts; global format settings are not universal selectors for the modern micro/SQL workloads.

## Findings to address

1. The connection check runs `hdfs dfs -ls /`, which checks Hadoop's default filesystem rather than explicitly checking the configured `hdfs_uri`.
2. Logical HDFS nameservices (HA) and default-filesystem URIs are rejected by the current host-and-port validator.
3. Data/output prefixes are embedded in the classic adapter. Storage configuration must participate in dataset fingerprints when moved outside `target`, so reuse cannot select data from another location.
4. Event logs, benchmark outputs, remote reports and controller state are different locations and must be displayed separately.
5. S3/GCS/ADLS support requires installed connectors, runtime identity/authentication, compatible writes and storage probes. Adding a provider dropdown alone does not implement support. Credentials should remain runtime-managed; experiments can refer to named connection profiles.

## Proposed navigation and delivery

Add **Parametrization → General** alongside **Workloads**, with sections **Storage**, **Logging and reports**, **Execution defaults**, and **Advanced configuration**. General settings provide experiment/profile defaults, with explicit experiment overrides included in exported YAML. Changing a default must not rewrite an existing run snapshot.

First implement configurable HDFS dataset/output roots, explicit Hadoop configuration location and accurate endpoint checks, preserving current defaults and compatibility with existing experiment files. Expose event logs, reuse policy and resources next. Object-storage providers can be shown as planned until their adapters/connectors have been validated.

Provider references: [Hadoop S3A](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html), [Hadoop ABFS](https://hadoop.apache.org/docs/r3.3.6/hadoop-azure/abfs.html), [Google Cloud Storage connector](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/README.md).

The table and findings above retain the initial review baseline; the delivered controls are described at the top. Current cluster defaults and existing stored data were preserved.

## Azure event-log clarification

The planned cloud dataset controls are a restriction of the current controller, not a limitation of Spark cloud event logs. `spark.eventLog.dir` already accepts a WASBS URI independently of the HDFS dataset endpoint and execution adapter. A runtime with the WASB connector and authentication can use it by changing that URI, as in the user's existing Azure deployment. No native Azure execution adapter is needed for this filesystem access.

`spark.history.fs.logDirectory` belongs to the separately running History Server; configure that server to read the same location. Supplying it in a job snapshot does not update an existing History Server. The local Docker cluster was not tested against an Azure account; the regression test proves URI preservation and separation from dataset storage.

References: [Hadoop Azure Blob Storage](https://hadoop.apache.org/docs/r3.3.6/hadoop-azure/index.html), [Spark monitoring](https://spark.apache.org/docs/3.5.7/monitoring.html).

## WASB/WASBS dataset support

General now offers Azure Blob Storage (WASB/WASBS) as an available backend for classic local/Docker submission against a runtime with the connector and authentication configured. Dataset and output base URIs may use that endpoint; storage probes and workload I/O pass the URI through Hadoop filesystem commands. The persisted `hdfs_uri` field name is retained for experiment-file compatibility, although it can now contain WASB/WASBS. ADLS ABFS/ABFSS remains a separate planned option. Azure runtime integration was not tested from this environment.
