# Generator migration acceptance policy

Every generator migration must preserve the original benchmark's logical input volume and representative data characteristics. Reproducibility is additional, not a substitute for equivalence.

Before implementation, identify the original source/runtime and effective settings. Record vocabulary/cardinality, distributions, lengths, skew, schema, separators/encoding, format and partitioning. Establish how the original counts requested bytes, including whole-record overshoot and storage overhead. Never silently replace logical bytes with physical file bytes.

Validation must include original-reference functional comparisons, measured input volume and representative distribution checks. Small-data correctness alone does not certify scale equivalence. Freeze a versioned dataset contract and report actual bytes/records/partitions. Preserve original scale presets. If a new profile changes benchmark characteristics, name it separately rather than presenting it as the migrated original.

Accepted modernization may replace storage formats and execution APIs while retaining the benchmark's scale, representative distributions and algorithmic purpose. Document format changes and establish a HiBench Next performance baseline; historical I/O timings need not match. Once validated, remove the replaced implementation and dependencies used exclusively by it. Keep provenance and validation evidence, not an operational legacy generator.

WordCount and Sort preserve Hadoop 3.3.6 RandomTextWriter defaults. A deterministic seed is introduced while preserving random selection and distributions. TeraSort and file-based Repartition preserve the original TeraGen record bytes and counts in a new Parquet binary schema. See `micro-modernization.md` for the separate preserved in-memory Repartition profile.
