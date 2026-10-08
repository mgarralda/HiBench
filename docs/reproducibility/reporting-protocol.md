# Migration reporting protocol

Update this record set as part of each migration, before calling a workload complete. Preserve original settings and independent reference fixtures before deleting a replaced generator. Historical records are immutable evidence; subsequent corrections must explain what they supersede.

## Generator record

Record original and replacement source identity, source revision/hash, effective runtime, CLI and all size presets. Specify logical units: requested versus produced rows/bytes, whole-record overshoot, dimensions, vocabulary and cardinality. Distinguish logical payload, storage encoding/compression and measured physical bytes. Document metadata and whether unused generated fields were removed or retained.

Specify seed, RNG, partition-dependent streams, repeatability across partition counts, component allocation and rounding. Describe distributions, feature/label relationships, class balance, sparsity, duplicate and self-link rules, skew, bounds and numeric precision. Explain every intentional change and correction. A standard-normal or uniform replacement is not representative unless it preserves the original workload's relevant joint distribution.

Compare against an independent original reference: exact records when appropriate; statistical checks with explicit tolerances and sample sizes for nondeterministic originals. Check dimensions, logical volume and boundary cases. Preserve original preset definitions; distinguish checking the mathematical generation plan from actually generating a large preset. Record storage/partition changes as differences, even when accepted.

## Workload record

Record old and new algorithm APIs, schema and input/import boundaries. Explain estimator, solver, iteration, convergence, regularization, initialization, seed, train/test split, caching, metrics and output changes. Document effective versus obsolete parameters. Verify meaningful actions materialize computation; separate generation, import, model fitting, evaluation and end-to-end timings.

Mark algorithm changes with an explicit version. Do not claim identical runtime, coefficients or historical throughput when a solver, format or execution plan changes. Preserve the computational purpose and scale; for a new benchmark, name it separately rather than disguising it as the old algorithm.

## Evidence and acceptance

Store versioned experiment configurations, source/fixture hashes, build and test commands, runtime versions, run/application IDs, measured counts/bytes, metrics, logs and failure/correction notes that affect interpretation. Link verifier source to its output. Record what was not run, unresolved issues and the actual validation platform. A DataFrame migration does not certify serverless support.

Use explicit evidence levels: configuration/design checked; original-reference functionality checked; representative distribution checked; real-cluster execution checked; repeated performance study completed. Only claim the levels actually achieved. Smoke runs validate execution, not performance superiority or cross-platform reproducibility.

After acceptance, delete the old production generator and exclusive dependencies; retain historical provenance and small reference fixtures. Update the migration ledger, evidence inventory and recovery snapshot. Record removed options such as DAL as retired, not pending or supported.
