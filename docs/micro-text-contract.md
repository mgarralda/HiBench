# WordCount and Sort: RandomTextWriter-compatible generator v2

Version 1's fixed-width, nearly unique hexadecimal tokens were rejected as unrepresentative. Its validation proved functionality and physical byte size only. The v1 results remain historical evidence, not an accepted benchmark profile.

## Original reference

Apache Hadoop 3.3.6 RandomTextWriter: https://github.com/apache/hadoop/blob/rel/release-3.3.6/hadoop-mapreduce-project/hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples/RandomTextWriter.java

The replacement uses its exact 1000-entry vocabulary in the same order, uniform random word selection, 5–9 words in a key and 10–99 words in a value. Both sentences retain their trailing spaces. Text output consists of key, TAB, value, LF. Lengths, repeated words and compressibility follow the original profile rather than fixed-width hashes.

## Volume contract

Original totalbytes counts key/value bytes including spaces, excluding TAB/LF. Each mapper finishes a complete record after crossing its budget. Consequently physical files are slightly larger, especially for tiny custom inputs. The migration retains this behavior; it does not truncate records to enforce exact physical bytes.

With requested total B and requested partitions P, bytespermap=floor(B/P); actual map count=floor(B/bytespermap), as in the original launch. The supported benchmark input minimum is 2 logical bytes, and B must be at least P. Each generated map consumes its budget in full records. Physical size equals logical payload size plus two bytes per record. The existing tiny/small/large/huge/gigantic/bigdata presets are unchanged.

## Spark implementation

Generation uses SparkSession.range and a typed Dataset flatMap with a lazy iterator per map ID, without explicit SparkContext, RDD or MapReduce. Keeping the original per-map stopping rule requires this sequential record iterator. A deterministic java.util.Random seed of seed+map ID is introduced; the original mapper used an unseeded java.util.Random. Repeating size/partition count/seed reproduces the multiset. Changing partitions can change records and overshoot, as in the original; partition-invariant content is no longer promised.

WordCount and Sort use SparkSession/DataFrame text readers and Parquet/noop writers. Generation and benchmark timing remain separate. No serverless/Spark Connect certification is claimed for typed Dataset closures or the classic launcher.

## Acceptance evidence

The Scala integration check compares the vocabulary and exact generated records with the original RandomTextWriter mapper's private generateSentence method from the installed Hadoop 3.3.6 examples JAR, using matching deterministic seeds. The Python verifier independently implements java.util.Random's 48-bit generator and checks expected records plus physical part-file sizes. verify_micro checks WordCount and Sort outputs separately.

See [the migration policy](generator-migration-policy.md). Scale throughput performance is not inferred from these correctness checks.

## Storage-format limit

The Git baseline preparation scripts did not specify an output format; RandomTextWriter defaults to SequenceFile. This modernization uses TextOutputFormat-equivalent text for the DataFrame benchmarks. The reference content and logical-volume contract are preserved, but the historical SequenceFile container overhead and reader cost are not reproduced. Therefore this validation must not be presented as complete storage-format or historical throughput equivalence. Format changes need explicit documentation under the migration policy.

## Validation results, 2026-10-07

All 11 Java 11 modules built; 2 Java and 19 Python tests passed. The four new YARN preparation/benchmark applications completed successfully. Original-JAR mapper comparisons matched exact records for 32,000 bytes/2 maps/seed42; 320,000 bytes/4 maps/seed43; 257 bytes/2 maps/seed44; and 2 bytes/2 maps/seed42.

| Dataset | Requested logical bytes | Actual logical bytes | Part-file bytes | Records | Words | Observed vocabulary |
|---|---:|---:|---:|---:|---:|---:|
| Original MapReduce reference | 32000 | 32425 | 32517 | 46 | 3042 | 957 |
| New WordCount input, seed43 | 32000 | 33564 | 33674 | 55 | 3161 | 960 |
| New Sort input, seed44 | 32000 | 32955 | 33065 | 55 | 3106 | 948 |

The original reference is unseeded, so matching sample record counts is not required. The same sampling/word-length distributions and stopping rule are required and checked. Each file's logical size crosses its 16,000-byte budget by less than one maximum-size record. File bytes include TAB/LF but exclude HDFS replication and filesystem metadata. WordCount counts and Sort content/global ordering passed independent checks.

[Validation JSON](micro-text-v2-validation-2026-10-07.json). The original scale preset values remain unchanged.
