# SQL modernization

Scan, Aggregation and Join now use Spark SQL over Parquet with session-local views. The runners and generator use SparkSession and Dataset/DataFrame APIs. Hive tables, a persistent metastore and Hive SerDes are unnecessary for these queries.

The replacement generator preserves the historical page and visit presets, map-slot seeds, URL lengths, Gaussian link counts, Zipf exponent 0.5, reducer seeds, weighted vocabularies, date range and revenue/duration distributions. The vocabularies were frozen from the historical generator on the reference cluster (JDK 11). Its effective US-ASCII decoding is captured, including replacement characters in a few search terms; the replacement loads this fixed content as UTF-8 instead of depending on the host charset. Dates use UTC explicitly to avoid host timezone drift.

`rankings` contains `pageId: long`, `pageURL: string`, `pageRank: int`, `avgDuration: int`. `uservisits` contains `pageId: long`, `sourceIP: string`, `destURL: string`, `visitDate: date`, `adRevenue: double`, `userAgent: string`, `countryCode: string`, `languageCode: string`, `searchWord: string`, `duration: int`. Page IDs preserve the historical SequenceFile keys for verification; queries project the original value columns.

Visits targeting a page with no incoming links are dropped, as in the original generator. Configured visits therefore describe candidates, not an unconditional output row count. Generation partitions and reducer count affect deterministic content just as in the original implementation.

Parquet replaces SequenceFile/CSV values. Physical bytes and decoding cost change; logical scale and representative content are the compatibility contract. Old inputs need regeneration or an explicit conversion; these readers do not silently reinterpret legacy SequenceFiles.

Join retains the inclusive 1999-01-01 through 2000-01-01 filter, URL inner join, source-IP grouping, average page rank, revenue sum and descending revenue sort. Scan projects nine visit columns; Aggregation groups by source IP and sums revenue.

## Validation status

Historical tiny reference captured: 120 rankings and 1,000 visits, with two generation maps and two reducers. The independent comparison script checks every field and original key. The isolated module compiled against Spark 3.5.9. Every field and key matches the historical tiny input. Independently computed query results match Scan (1,000 rows), Aggregation (1,000 groups) and Join (21 groups), with an in-memory catalog. The final Java 11 Maven build passed all 11 modules and two Java tests; 27 Python tests pass. All six preparation/run applications succeeded on YARN using the final bundle. Independent verification of every persisted input and query output passed, including duplicate groups, inclusive date boundaries, unmatched URLs, averages and descending revenue order. Cluster doctor and Streamlit health checks pass. Cloud/serverless compatibility is not certified.

The SQL-specific MapReduce generator (`HiveData`, `Visit`, `JoinBytesInt`) and its CLI option have been removed. Shared legacy generation utilities remain because other workload families still use them. Hive script generation, metastore/Guava setup and recursive permission adjustment have been removed from the SQL launch path.

Original presets are frozen in `docs/sql-scale-reference.json`, generator provenance and vocabulary hashes in `docs/sql-original-reference.json`, and the complete tiny reference in `tests/fixtures/sql-original-tiny.json`. Validation commands use `configs/smoke-sql.yaml` and `tests/integration/verify_sql.py`.

The Zipf kernels were compared against the historical jar for all eight distinct preset page counts (120 through 120,000,000), with 60,000 draws each: all 480,000 draws match. This checks the distribution algorithm at large scales without claiming a full bigdata execution.

For the tiny reference, SequenceFile occupies 194,847 bytes and uncompressed Parquet 80,168 bytes. Parquet dictionary/column encoding still reduces space when compression is disabled. These are storage bytes, separate from identical logical records and fields.

Existing benchmark reports derive throughput from physical input bytes. Compare elapsed times and logical record counts when assessing a migration between storage formats; physical-byte throughput is suitable for comparisons using the same format and generator version.

Final runtime proof, application IDs, source/artifact hashes and test limits are recorded in [the SQL validation report](sql-modern-validation-2026-10-08.json). Controller run: `135b7a2e-39d0-4265-92ab-eba5ac23e976`.
