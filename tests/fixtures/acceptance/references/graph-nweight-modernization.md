<!--
Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# NWeight modernization — 2026-10-09

The versioned workload `weighted-path-topk-v1` implements immutable weighted path propagation with DataFrame joins, aggregation and top-K windows. Original GraphX/Pregel sources are removed from production and preserved as text provenance. Their 48 captured results remain in `tests/fixtures/nweight-original-profile.json`. There is one modern algorithm, not two old backend aliases. Old `model`, `disable_kryo`, storage-level and unused map-partition options are retired; saved experiments specifying removed parameters must be updated rather than silently ignored.

## Corrected algorithm contract

All input vertices appearing as source or destination are retained in output, with an empty target list when there are no contributions. At depth 1, parallel edges to the same destination are summed; retain the greatest K weights for each source. At each subsequent depth, multiply every original edge u→x by retained contributions x→v, sum contributions for (u,v), exclude paths returning to u, then apply top-K again. Rank by descending weight, with greatest destination ID breaking ties, following the old queue's comparator. Ranking is by signed weight, not absolute magnitude. The raw multiset of edges remains the propagation input, so parallel paths still contribute.

This is truncated propagation: top-K pruning at each depth can discard paths that would matter later. It is not a full all-path matrix power when K is small. Zero-weight entries are retained and participate in ranking. We test negative weights, self-links, duplicate edges, ties, chains and cycles independently.

Historical behavior is not a reliable equivalence target. GraphX's update mutates queues already attached to cached graph vertices; the captured chain returns the same depth-2 and depth-3 state. Pregel starts from a shared mutable default queue and produces inconsistent per-vertex attributes in the tiny references. These are recorded implementation defects, not intended mathematical semantics. The corrected depth-3 chain returns u=0→v=3 with weight 24, rather than the old depth-2 contribution 0→2 with weight 6. Parallel-edge aggregation and deterministic tie handling are explicit version changes. Historical timings and erroneous results are not directly comparable.

## Generator contract

The original `edges` property counts sampled ordered pairs, not stored directed rows. Each pair is emitted in both directions. The six preserved pair presets are 100,000; 1,000,000; 10,000,000; 100,000,000; 425,000,000; and 4,250,000,000. Corresponding Parquet row counts are twice those values. Logical feature payload is 24 bytes per directed edge (two Long IDs and one Double weight), excluding encoding and metadata.

Endpoints are uniform over the original 2,401,080 IDs, rejecting self-pairs. The original big-endian float model file is unchanged. Weight is the product of its two scalar features, quantized to four decimal places with Java formatting, exactly the original rank-one MatrixFactorizationModel prediction/text boundary. Zero features and weights are retained; no new graph degree law or weight distribution is substituted.

The new seed defaults to 42; the old generator used global nondeterministic Scala Random. Spark SQL endpoint RNG fields use disjoint seed families. Consecutive seeds would overlap across neighboring Spark partitions and create artificial connectivity, so the second field uses seed XOR 0x5DEECE66D. Both seed and shuffle partitions are part of reproducibility.

Generate 20% extra candidates, remove duplicate ordered pairs globally, and check there are enough. Select exactly the requested Long pair count using hash buckets, independent hash ordering and Long cumulative counts. Sorting the final partial bucket by source ID would bias node sampling, so selection instead uses a second hash. Symmetric expansion can still produce parallel directed edges when opposite ordered pairs were sampled; that multiplicity is retained. The old per-batch distinct/union logic could retain duplicates across retry batches and could stall when floor(remaining × 1.2 / partitions) became zero; neither defect is retained.

The supported pair budget covers all original presets. The biggest profile needs more shuffle partitions to keep per-task window counts inside Spark/JVM limits; validation rejects a two-partition bigdata setup and accepts four. Definitions and mathematical volume are preserved, not a claim that the local cluster can execute billions of edges.

The launcher uploads the same fixed model file through Hadoop filesystem operations to a temporary URI for Spark's binary reader, then removes that staging file. This is a storage copy, not a MapReduce job. Generation and propagation use SparkSession/DataFrame/Dataset APIs. The hidden profile records pair and directed-edge budgets, seed, partitions, model SHA-256 and generator version. Old text adjacency lists become typed Parquet `src LONG, dst LONG, weight DOUBLE`.

## Acceptance evidence

Independent Python recurrence comparisons cover 24 cases (four graph fixtures × three depths × two K values). Historical outputs are recorded separately and are never used as a production fallback. Actual tiny/custom generated graphs are checked for exact row count, endpoint range, self-pair exclusion, symmetry with multiplicity, model-derived weights and quantization, node histogram and occupied-node count. Reference-only collection is bounded to tiny; the workload never collects graph input to the driver.

Result Parquet has `id LONG` and `targets ARRAY<STRUCT<dst:LONG,weight:DOUBLE>>`. Immutable cached state is materialized each depth, and cache ownership is explicitly released after writing. This fixes correctness and changes physical action/storage costs. See `graph-nweight-validation-2026-10-09.json` and the migration ledger for accepted YARN runs and limitations. No cloud/serverless certification is implied.
