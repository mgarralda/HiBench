# Workload parameterization

The sidebar groups reference configuration under **Parametrization → Workloads**. Every maintained workload shows its six reference scales, workload-specific fixed parameters, descriptions and additional configuration expressions (derived partition settings and runtime paths).

The editor applies values to the current experiment, adds the chosen workload if necessary, and uses the existing experiment validation. Only differences from the chosen preset are stored. Applying a scale changes the global experiment scale; explicit overrides on other workloads remain explicit overrides. Original configuration files and reference presets remain unchanged.

Customize values with **Edit parameters**, then **Use in experiment**. Open **Experiments** to review, export YAML or execute. Navigating away from Experiments preserves a valid draft. No benchmark starts from the Workloads page.

Boolean scale expressions and quoted memory settings are normalized into typed controls. PageRank block settings remain visible as legacy configuration but are disabled in this editor because the current Spark launchers do not consume them. Workload inspection does not certify untested algorithms or cloud runtimes.

Global preset editing would require a separate versioned preset/profile mechanism. Experiment overrides already support reproducible customization without changing the community reference.
