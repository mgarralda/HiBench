# Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""One schema for UI, CLI and persisted experiment snapshots."""
import copy
import json
from pathlib import Path
import re
import yaml
from .catalog import SCALES, workloads
from .adapters import adapter_info, create_adapter


def load(path):
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return resolve(data)


def resolve(overrides):
    """Apply an imported experiment or a compact override to the conf/ base."""
    if not isinstance(overrides, dict):
        raise ValueError('Experiment overrides must be an object')
    base = default_experiment(scale=overrides.get('scale'))
    for key, value in overrides.items():
        if key == 'target' and isinstance(value, dict) and {'workspace', 'spark_home', 'hadoop_home', 'master', 'hdfs_uri'} <= set(value):
            base[key] = copy.deepcopy(value)
        elif key in ('target', 'resources', 'spark_conf', 'monitoring') and isinstance(value, dict):
            base[key] = {**base.get(key, {}), **copy.deepcopy(value)}
        else:
            base[key] = copy.deepcopy(value)
    if base['target'].get('kind') != 'docker':
        base['target'].pop('container', None)
    return validate(base)


def overrides(experiment):
    """Export differences only; the user owns the downloaded file."""
    base = default_experiment(scale=experiment['scale'])
    result = {'schema_version': 1}
    if experiment['scale'] != default_experiment()['scale']:
        result['scale'] = experiment['scale']
    for key, value in experiment.items():
        if key == 'schema_version':
            continue
        if key == 'target' and value != base[key]:
            # A complete target preserves removals as well as changed values.
            result[key] = copy.deepcopy(value)
        elif isinstance(value, dict) and isinstance(base.get(key), dict):
            changes = {k: v for k, v in value.items() if base[key].get(k) != v}
            if changes:
                result[key] = changes
        elif value != base.get(key):
            result[key] = copy.deepcopy(value)
    return result


def scalar(value):
    return str(value).lower() if isinstance(value, bool) else str(value)


def default_experiment(conf_dir=None, scale=None, selected_workloads=None):
    """Build a draft from the sole maintained configuration base in conf/."""
    from .catalog import repository_root
    conf_dir = Path(conf_dir) if conf_dir else repository_root() / 'conf'
    values = {}
    for path in sorted(conf_dir.glob('*.conf')):
        if path.name == 'spark.conf':
            from .parameters.spark import parse_spark
            values.update(parse_spark(path.read_text(encoding='utf-8')))
            continue
        for line in path.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                parts = line.split(None, 1)
                if len(parts) == 2:
                    values[parts[0]] = parts[1].strip()
    selected_scale = scale or values['hibench.scale.profile']
    from .parameters.spark import scale_values
    values, _ = scale_values(values, selected_scale)
    def required(key):
        if key not in values:
            raise ValueError('Missing base configuration property: ' + key)
        return values[key]
    target = dict(kind=required('hibench.controller.adapter'),
                  workspace=required('hibench.controller.workspace'),
                  spark_home=required('hibench.spark.home'), hadoop_home=required('hibench.hadoop.home'),
                  master=required('hibench.spark.master'), deploy_mode=required('hibench.yarn.deploy.mode'),
                  hdfs_uri=required('hibench.hdfs.master'), workers=required('hibench.slaves.hostnames').split())
    if target['kind'] == 'docker':
        target['container'] = required('hibench.masters.hostnames').split()[0]
    elif target['kind'] == 'ssh':
        target.update(host=required('hibench.controller.ssh.host'),
                      username=required('hibench.controller.ssh.username'),
                      port=int(values.get('hibench.controller.ssh.port', '22')))
        for key in ('identity_file', 'known_hosts_file'):
            if values.get('hibench.controller.ssh.' + key):
                target[key] = values['hibench.controller.ssh.' + key]
    hadoop_conf = values.get('hibench.hadoop.configure.dir')
    if hadoop_conf:
        target['hadoop_conf_dir'] = hadoop_conf.replace('${hibench.hadoop.home}', target['hadoop_home'])
    resources = dict(executor_instances=int(required('spark.executor.instances')),
                     executor_cores=int(required('spark.executor.cores')),
                     executor_memory=required('spark.executor.memory'), driver_memory=required('spark.driver.memory'),
                     generation_partitions=int(required('hibench.generator.partitions')),
                     generation_shuffle_partitions=int(required('hibench.generator.shuffle.partitions')),
                     shuffle_partitions=int(required('spark.sql.shuffle.partitions')))
    selected = [line.strip() for line in (conf_dir / 'benchmarks.lst').read_text(encoding='utf-8').splitlines()
                if line.strip() and not line.strip().startswith('#')]
    return validate(dict(schema_version=1, name='Batch experiment', target=target,
                         resources=resources, spark_conf={}, scale=selected_scale,
                         implementation=required('hibench.workload.implementation'),
                         mode=required('hibench.controller.mode'),
                         repetitions=int(required('hibench.controller.repetitions')),
                         timeout_s=int(required('hibench.controller.timeout.seconds')),
                         monitoring={'enabled': required('hibench.monitor.enabled') == '1',
                                     'interval_s': int(required('hibench.monitor.interval.seconds'))},
                         workloads=[{'id': name, 'parameters': {}} for name in (selected_workloads or selected)]))


def validate(data):
    if not isinstance(data, dict):
        raise ValueError("Experiment must be an object")
    data = copy.deepcopy(data)
    allowed = {"schema_version", "name", "target", "mode", "scale", "repetitions", "timeout_s",
               "resources", "spark_conf", "workloads", "monitoring", "execution_group", "implementation"}
    if set(data) - allowed:
        raise ValueError("Unknown experiment fields: " + ", ".join(sorted(set(data) - allowed)))
    if data.get("schema_version") != 1:
        raise ValueError("schema_version must be 1")
    group = data.setdefault("execution_group", "")
    if not isinstance(group, str) or len(group) > 128 or any(ord(char) < 32 or ord(char) == 127 for char in group):
        raise ValueError("execution_group must be a string of up to 128 characters without control characters")
    data["execution_group"] = group.strip()
    implementation = data.setdefault("implementation", "scala")
    if implementation not in ("scala", "pyspark", "both"):
        raise ValueError("implementation must be scala, pyspark or both")
    target = data.get("target", {})
    if not isinstance(target, dict):
        raise ValueError("target must be an object")
    adapter_info(target.get("kind"))
    create_adapter(target).validate_target()
    if implementation == "pyspark" and target["deploy_mode"] != "client":
        raise ValueError("PySpark-only experiments require client deploy mode")
    monitoring = data.setdefault("monitoring", {})
    if not isinstance(monitoring, dict) or set(monitoring) - {"enabled", "interval_s"}:
        raise ValueError("monitoring accepts enabled and interval_s only")
    monitoring.setdefault("enabled", False)
    monitoring.setdefault("interval_s", 5)
    if type(monitoring["enabled"]) is not bool:
        raise ValueError("monitoring.enabled must be boolean")
    if type(monitoring["interval_s"]) is not int or not 1 <= monitoring["interval_s"] <= 60:
        raise ValueError("monitoring.interval_s must be between 1 and 60")
    data.setdefault("mode", "prepare_and_run")
    if data["mode"] not in ("prepare", "run", "prepare_and_run"):
        raise ValueError("Invalid mode")
    data.setdefault("scale", "tiny")
    if data["scale"] not in SCALES:
        raise ValueError("Invalid scale")
    for key, default, maximum in (("repetitions", 1, 100), ("timeout_s", 1800, 604800)):
        data.setdefault(key, default)
        if type(data[key]) is not int or not 1 <= data[key] <= maximum:
            raise ValueError(f"{key} must be between 1 and {maximum}")
    resources = data.setdefault("resources", {})
    if not isinstance(resources, dict):
        raise ValueError("resources must be an object")
    defaults = dict(executor_instances=1, executor_cores=1, executor_memory="1g",
                    driver_memory="1g", generation_partitions=2, generation_shuffle_partitions=200,
                    shuffle_partitions=2)
    if set(resources) - set(defaults):
        raise ValueError("Unknown resource field")
    for key, value in defaults.items():
        resources.setdefault(key, value)
        if key.endswith("memory"):
            if not isinstance(resources[key], str) or not re.fullmatch(r"[1-9]\d*[mg]", resources[key]):
                raise ValueError(f"Invalid memory: {key}")
        elif type(resources[key]) is not int or not 1 <= resources[key] <= 10000:
            raise ValueError(f"Invalid resource: {key}")
    conf = data.setdefault("spark_conf", {})
    if not isinstance(conf, dict):
        raise ValueError("spark_conf must be an object")
    reserved = {"spark.extraListeners", "spark.master", "spark.app.name", "spark.submit.deployMode",
                "spark.executor.instances", "spark.executor.cores", "spark.executor.memory",
                "spark.driver.memory", "spark.sql.shuffle.partitions",
                "spark.yarn.tags", "spark.app.benchmark.group.id", "spark.app.benchmark.workload",
                "spark.app.benchmark.data.size", "spark.yarn.submit.waitAppCompletion"}
    for key, value in conf.items():
        if not re.fullmatch(r"spark\.[\w.]+", key) or key in reserved:
            raise ValueError(f"Use the resource/target fields for reserved Spark property: {key}")
        if not isinstance(value, (str, int, float, bool)) or "\n" in scalar(value) or "\r" in scalar(value):
            raise ValueError(f"Invalid property value: {key}")
    catalog = {item["id"]: item for item in workloads()}
    selected = data.get("workloads", [])
    if not isinstance(selected, list) or not selected:
        raise ValueError("Select at least one workload")
    seen = set()
    for item in selected:
        if not isinstance(item, dict) or set(item) - {"id", "parameters"} or item.get("id") not in catalog:
            raise ValueError("Invalid batch workload")
        if item["id"] in seen:
            raise ValueError("Duplicate workload")
        seen.add(item["id"])
        parameters = item.setdefault("parameters", {})
        if not isinstance(parameters, dict):
            raise ValueError("parameters must be an object")
        schema = catalog[item["id"]]["parameters"]
        for key, value in parameters.items():
            if key not in schema:
                raise ValueError(f"Unknown parameter {key} for {item['id']}")
            if item["id"] in ("micro.wordcount", "micro.sort") and key == "datasize" and (type(value) is not int or value < 2):
                raise ValueError("Text dataset size must be at least 2 bytes")
            if item["id"] in ("micro.wordcount", "micro.sort") and key == "datasize" and value < resources["generation_partitions"]:
                raise ValueError("Logical bytes must be at least the number of generation partitions")
            if item["id"] in ("micro.terasort", "micro.repartition") and key == "datasize" and (type(value) is not int or not 1 <= value <= (2**63 - 1) // 100):
                raise ValueError("Tera dataset size must be a positive record count within the logical byte limit")
            if item["id"] == "micro.repartition" and key == "datasize" and parameters.get("fromhdfs", True) is False and value > (2**63 - 1) // resources["generation_partitions"] // 200:
                raise ValueError("In-memory Repartition logical size exceeds the supported limit")
            if item["id"].startswith("sql.") and key in ("pages", "uservisits"):
                limit = (2**63 - 1) // 40 if key == "pages" else 2**63 - 1 - resources["generation_partitions"] - 1
                minimum = 2 if key == "pages" else 1
                if type(value) is not int or not minimum <= value <= limit:
                    raise ValueError(f"Invalid SQL {key}: supported range is {minimum} to {limit}")
            kind = schema[key]["kind"]
            if kind == "integer" and (type(value) is not int or value < 0):
                raise ValueError(f"{key} must be a nonnegative integer")
            if item["id"].startswith("micro.") and kind == "integer" and value > 2**63 - 1:
                raise ValueError(f"{key} exceeds the supported 64-bit limit")
            if item["id"] == "micro.sleep" and key == "mapper.seconds" and value > (2**63 - 1) // 1000:
                raise ValueError("Sleep duration exceeds the supported millisecond limit")
            if kind == "number" and (type(value) not in (int, float) or value < 0):
                raise ValueError(f"{key} must be nonnegative numeric")
            if kind == "boolean" and type(value) is not bool:
                raise ValueError(f"{key} must be boolean")
            if kind == "string" and (not isinstance(value, str) or not re.fullmatch(r"[\w.|-]+", value)):
                raise ValueError(f"Invalid parameter string: {key}")
        if item["id"] == "websearch.pagerank":
            pages = parameters.get("pages", int(schema["pages"]["presets"][data["scale"]]))
            seed = parameters.get("seed", int(schema["seed"]["default"]))
            iterations = parameters.get("num_iterations", int(schema["num_iterations"]["presets"][data["scale"]]))
            if type(pages) is not int or not 2 <= pages <= (2**63 - 1) // 40:
                raise ValueError("Invalid PageRank node count")
            if type(seed) is not int or not 1 <= seed <= 2**31 - resources["generation_partitions"]:
                raise ValueError("PageRank slot seed exceeds Int range")
            if type(iterations) is not int or not 0 <= iterations <= 2**31 - 1:
                raise ValueError("Invalid PageRank iteration count")
        from .parameters.ml import validate_pending_ml
        validate_pending_ml(item["id"], parameters, schema, data["scale"], resources)
        if item["id"] == "ml.svd":
            from .parameters.svd import validate_svd
            validate_svd(parameters, schema, data["scale"])
        if item["id"] == "graph.nweight":
            from .parameters.nweight import validate_nweight
            validate_nweight(parameters, schema, data["scale"], resources)
        if item["id"] in ("ml.pca", "ml.correlation", "ml.summarizer"):
            values = {key: parameters.get(key, int(schema[key]["presets"][data["scale"]]))
                      for key in ("examples", "features", "seed")}
            for key, value in values.items():
                maximum = 2**31 - 1 if key == "features" else 2**63 - 1
                minimum = 0 if key == "seed" else (2 if key == "examples" else 1)
                if type(value) is not int or not minimum <= value <= maximum:
                    raise ValueError(f"Statistics {key} must be an integer between {minimum} and {maximum}")
            if item["id"] != "ml.summarizer" and values["seed"] + values["examples"] - 1 > 2**63 - 1:
                raise ValueError("Statistics row seed exceeds Long range")
            if item["id"] == "ml.pca":
                k = parameters.get("k", int(schema["k"]["presets"][data["scale"]]))
                if type(k) is not int or not 1 <= k <= values["features"]:
                    raise ValueError("PCA k must be between 1 and the feature dimension")
            if item["id"] == "ml.correlation":
                if parameters.get("corrType", schema["corrType"]["default"]) not in ("pearson", "spearman"):
                    raise ValueError("Correlation method must be pearson or spearman")
        if item["id"] in ("ml.rf", "ml.gbt"):
            family = item["id"].split('.')[1]
            keys = ("examples", "features", "seed", "split_seed", "model_seed", "numClasses", "maxDepth", "maxBins")
            effective = {key: parameters.get(key, int(schema[key]["presets"][data["scale"]])) for key in keys}
            for key, value in effective.items():
                limit = 2**63 - 1 if key in ("examples", "seed", "split_seed", "model_seed") else 2**31 - 1
                minimum = 0 if key in ("seed", "split_seed", "model_seed", "maxDepth") else (2 if key in ("examples", "numClasses", "maxBins") else 1)
                if type(value) is not int or not minimum <= value <= limit:
                    raise ValueError(f"Tree {key} must be an integer between {minimum} and {limit}")
            if effective["maxDepth"] > 30:
                raise ValueError("Tree maxDepth must not exceed 30")
            if effective["seed"] + effective["examples"] - 1 > 2**63 - 1:
                raise ValueError("Tree generation row seed exceeds the supported Long range")
            fraction = parameters.get("train_fraction", float(schema["train_fraction"]["default"]))
            if not 0 < fraction < 1:
                raise ValueError("Tree train_fraction must be in (0, 1)")
            if family == "gbt":
                iterations = parameters.get("numIterations", int(schema["numIterations"]["default"]))
                rate = parameters.get("learningRate", float(schema["learningRate"]["default"]))
                if effective["numClasses"] != 2 or iterations < 1 or iterations > 2**31 - 1 or not 0 < rate <= 1:
                    raise ValueError("GBT requires two classes, positive iterations and learningRate in (0, 1]")
            else:
                trees = parameters.get("numTrees", int(schema["numTrees"]["default"]))
                strategy = parameters.get("featureSubsetStrategy", schema["featureSubsetStrategy"]["default"])
                impurity = parameters.get("impurity", schema["impurity"]["default"])
                if not 1 <= trees <= 2**31 - 1:
                    raise ValueError("RF numTrees must be positive within the Int range")
                if strategy not in ("auto", "all", "sqrt", "log2", "onethird") or impurity not in ("gini", "entropy"):
                    raise ValueError("Unsupported RF featureSubsetStrategy or classification impurity")
        if item["id"] == "ml.lr":
            keys = ("examples", "features", "seed", "split_seed", "num_iterations")
            values = {key: parameters.get(key, int(schema[key]["presets"][data["scale"]])) for key in keys}
            for key, value in values.items():
                limit = 2**63 - 1 if key in ("examples", "seed", "split_seed") else 2**31 - 1
                minimum = 0 if key in ("seed", "split_seed") else (2 if key == "examples" else 1)
                if type(value) is not int or not minimum <= value <= limit:
                    raise ValueError(f"Logistic {key} must be an integer between {minimum} and {limit}")
            if values["seed"] + values["examples"] - 1 > 2**63 - 1:
                raise ValueError("Logistic row seed exceeds the supported Long range")
            fraction = parameters.get("train_fraction", float(schema["train_fraction"]["default"]))
            threshold = parameters.get("threshold", float(schema["threshold"]["default"]))
            tolerance = parameters.get("tolerance", float(schema["tolerance"]["default"]))
            if not 0 < fraction < 1 or not 0 <= threshold <= 1 or tolerance <= 0:
                raise ValueError("Logistic requires 0 < train_fraction < 1, 0 <= threshold <= 1 and tolerance > 0")
        if item["id"] == "ml.linear":
            for key in ("examples", "features", "seed", "num_iterations"):
                value = parameters.get(key, int(schema[key]["presets"][data["scale"]]))
                limit = 2**63 - 1 if key in ("examples", "seed") else 2**31 - 1
                minimum = 0 if key == "seed" else 1
                if type(value) is not int or not minimum <= value <= limit:
                    raise ValueError(f"Linear {key} must be an integer between {minimum} and {limit}")
            alpha = parameters.get("elasticnet_param", float(schema["elasticnet_param"]["default"]))
            if not 0 <= alpha <= 1:
                raise ValueError("Linear elasticnet_param must be between 0 and 1")
            tol = parameters.get("tolerance", float(schema["tolerance"]["default"]))
            if tol <= 0:
                raise ValueError("Linear tolerance must be positive")
            fraction = parameters.get("test_fraction", float(schema["test_fraction"]["default"]))
            if not 0 <= fraction < 1:
                raise ValueError("Linear test_fraction must be in [0, 1)")
        if item["id"] == "ml.als":
            effective = {key: parameters.get(key, int(schema[key]["presets"][data["scale"]]))
                         for key in ("users", "products", "ratings")}
            for key, value in effective.items():
                limit = 2**63 - 1 if key == "ratings" else 2**31 - 1
                if type(value) is not int or not 1 <= value <= limit:
                    raise ValueError(f"ALS {key} must be between 1 and {limit}")
            if effective["ratings"] > effective["users"] * effective["products"]:
                raise ValueError("ALS ratings must not exceed users * products")
        if item["id"] in ("ml.kmeans", "ml.gmm"):
            keys = ("num_of_samples", "num_of_clusters", "dimensions", "samples_per_inputfile", "k", "max_iteration", "seed")
            effective = {key: parameters.get(key, int(schema[key]["presets"][data["scale"]])) for key in keys}
            for key, value in effective.items():
                limit = 2**63 - 1 if key in ("num_of_samples", "samples_per_inputfile", "seed") else 2**31 - 1
                minimum = 0 if key == "seed" else 1
                if type(value) is not int or not minimum <= value <= limit:
                    raise ValueError(f"Clustering {key} must be an integer between {minimum} and {limit}")
            if effective["k"] > effective["num_of_samples"]:
                raise ValueError("Clustering k must not exceed the sample count")
            if effective["num_of_clusters"] * effective["dimensions"] > 1_000_000:
                raise ValueError("Mixture metadata exceeds one million cluster/feature pairs")
            ranges = {key: parameters.get(key, float(schema[key]["presets"][data["scale"]]))
                      for key in ("mean_min", "mean_max", "std_min", "std_max")}
            if not ranges["mean_min"] < ranges["mean_max"] or not 0 < ranges["std_min"] < ranges["std_max"]:
                raise ValueError("Clustering requires mean_min < mean_max and 0 < std_min < std_max")
    json.dumps(data, allow_nan=False)
    create_adapter(target).validate(data)
    return data
