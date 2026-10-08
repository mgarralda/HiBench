# HiBench Next micro workloads

The five supported micro workloads use SparkSession with DataFrames/Datasets.
Their preparation and Spark execution no longer require MapReduce or the Hadoop
examples/test JARs. The old implementations are removed from the active source
tree; Git retains their history. Other workload families may still need legacy
generators and dependencies until their own migrations are validated.

| Workload | Input and generation | Work performed |
| --- | --- | --- |
| WordCount | Original RandomTextWriter vocabulary, word-count distributions and logical byte stopping rule; text | Split nonempty words and aggregate counts |
| Sort | Same text generation contract | Globally order complete text records |
| TeraSort | Original Hadoop 3.3.6 TeraGen records; Parquet `key: binary`, `value: binary` | Globally order by the 10-byte key using unsigned binary order |
| Repartition | Same TeraGen data, or the preserved in-memory profile | Shuffle into the configured partition count; optional cache and output |
| Sleep | No external dataset; one range row per task | Wait the configured seconds in each task and verify every task completes |

## Tera input contract

Every record retains exactly 100 logical bytes: a 10-byte key and a 90-byte
value. The 128-bit generator, row identifier, delimiters and filler are adapted
from Apache Hadoop 3.3.6 GenSort, Random16 and Unsigned16, with ASF attribution.
Rows are generated for identifiers `0 .. N-1`. Changing generation partitions
does not change their multiset. No seed parameter changes the original TeraGen
sequence. The new generator has no Hadoop example classes or MapReduce jobs.

The original raw 100-byte container is replaced by uncompressed Parquet binary
columns. Logical payload is exactly `100 * N`; physical Parquet bytes include
metadata and encoding and need not equal that payload. Binary keys remain
binary, so their cardinality and byte distribution are preserved. Old raw Tera
files must be regenerated or imported explicitly; they are not silently read
as the new format. This accepted format change establishes a new performance
baseline rather than reproducing historical reader/writer costs.

All scale presets retain their original record counts. The original presets for all five micro workloads are frozen in
`micro-scale-reference.json` and checked by the regression suite.

| Scale | TeraSort records | Repartition records (file input) |
| --- | ---: | ---: |
| tiny | 32,000 | 3,200 |
| small | 3,200,000 | 3,200,000 |
| large | 32,000,000 | 32,000,000 |
| huge | 320,000,000 | 320,000,000 |
| gigantic | 3,200,000,000 | 3,200,000,000 |
| bigdata | 6,000,000,000 | 6,000,000,000 |

Repartition `fromhdfs=false` retains the legacy alternative profile: **N records
per generation partition**, each containing the same 200-byte sequence
`0 .. 199` modulo 256. Total payload is `N * generation_partitions * 200`, not
`N * 100`. It remains a separate in-memory benchmark mode. Its arbitrary legacy
three-second pre-cache delay is removed; cache materialization is retained.
Run-only mode does not require an external dataset in this mode.

Shuffle partitions now follow the explicit resource setting. TeraSort no longer
derives reducers from half the executor-core count. SQL adaptive execution can
coalesce partitions when enabled; disable it for comparisons requiring fixed
partition counts. Repartition uses Spark SQL's balanced round-robin shuffle.
Caching uses the DataFrame cache, whose layout and memory use follow Spark SQL
rather than the old RDD representation. Noop output still executes the shuffle
without writing files.

## Sleep contract

Sleep has no data preparation. Task count is `spark.default.parallelism`, supplied
by the controller's generation/map partition setting (standalone fallback: 2).
Each task sleeps once for the selected `mapper.seconds`. The unused legacy
reducer duration and Hadoop SleepJob probes are removed. Existing mapper-duration
presets remain unchanged. Retries/speculation may repeat waits, as with the old
workload; comparisons should use consistent scheduler settings.

## Scope and verification

`configs/smoke-micro.yaml` exercises all five workloads on Docker/YARN. Integration
checks compare the Tera generation algorithm with the original Hadoop JAR as a
**test fixture only**, verify record preservation and binary sort order, reject
malformed records, exercise cache/noop output and measure Sleep's actual wait.
Large scale presets retain their definitions but are not all materialized during
small-data validation. Typed Dataset execution is validated on classic Spark
3.5.9; this does not certify Spark Connect or Databricks serverless support.

See [the text generator contract](micro-text-contract.md) and
[the generator migration policy](generator-migration-policy.md).

Validated tiny inputs on the reference Spark 3.5.9 cluster:

| Input | Records | Logical payload bytes | Physical Parquet bytes |
| --- | ---: | ---: | ---: |
| Repartition | 3,200 | 320,000 | 347,605 |
| TeraSort | 32,000 | 3,200,000 | 3,458,400 |

An independent Python implementation of the original generator verified every
record in both inputs and checked preservation in the workload outputs. TeraSort
also passed within-partition and global unsigned binary ordering checks.
Benchmark `input_bytes`/throughput continue to describe physical input storage;
the logical payload and record count are recorded separately in validation.

Full evidence, successful YARN application IDs, artifact/source hashes and limits:
[micro-modern-validation-2026-10-07.json](micro-modern-validation-2026-10-07.json).
