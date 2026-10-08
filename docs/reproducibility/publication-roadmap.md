# Scientific reporting and reuse roadmap

The intended contribution is a maintained open-source Spark benchmark suite with preserved representative workload profiles, modern data contracts, observable execution and reproducible configuration. A possible SoftwareX article should substantiate this software contribution with archived code and evidence; publication suitability and submission requirements remain to be checked when preparing the manuscript.

## Proposed article structure

1. Motivation: legacy APIs, opaque launch/configuration paths, obsolete dependencies and the need for reusable Spark benchmarking.
2. Software architecture: versioned experiments, workload catalog, execution adapters, durable runs/logs, UI and shared Python services.
3. Migration methodology: profile preservation, independent references, explicit algorithm/format revisions and removal of obsolete implementations.
4. Validation: generator equivalence, workload correctness, Spark 3.5.9 execution and clearly delimited platform support.
5. Reproducibility and reuse: installation, companion Docker cluster, documented configurations, datasets/contracts and artifacts.
6. Evaluation and limitations: repeated experiments, resources, variability, format-related limits and workloads still pending.
7. Extensibility: additional Spark SQL workloads, execution adapters and programmatic API.

The recorded companion spark-hadoop cluster revision provides a directly tested environment. Describe it as a complementary deployment artifact with its own pinned revision, build/runtime versions and installation checks. It is not the only possible deployment target.

## Evidence still required for performance claims

Define research questions before measurements. Record hardware, container limits, resource allocation, dataset versions/seeds, storage formats, cache policy and Spark properties. Use repetitions and report dispersion, failed runs and warm-up treatment. Measure preparation separately from execution; report runtime, logical/physical throughput where interpretable, CPU/memory/shuffle/I/O and output checks. Compare equivalent workload purposes and disclose solver or format changes. Existing tiny smoke runs and generator checks do not establish faster performance, scalability at every preset or superiority over other benchmark suites.

Additional SQL workloads should have documented queries, input characteristics, correctness references and distinct profile versions. Select external benchmark comparisons and applicable licensing when those workloads are proposed; no specific suite is claimed as integrated yet.

## UI and programmatic access

The UI lowers the entry cost by exposing workload presets, cluster/storage settings, advanced Spark properties and durable run status/logs. Preserve the same versioned YAML/JSON experiment format for UI and programmatic use. User-facing configuration remains in English.

The future REST API should call the same validation and orchestration services as Streamlit. Candidate capabilities are catalog/preset discovery, experiment validation/import/export, run submission, status/log retrieval, cancellation and supported cluster-management operations. Preserve durable run IDs and idempotent submission; execution adapters remain the backend boundary. Move any remaining business logic from UI handlers into shared services before exposing equivalent routes. Do not implement a second submit/configuration path or let the API bypass existing validations.

REST implementation is future work; the current Python/controller interfaces and documented UI capabilities must not be described as an already released REST service. Record and test endpoint contracts and API versioning when implementation begins.
