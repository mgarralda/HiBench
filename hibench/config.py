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
    return validate(data)


def scalar(value):
    return str(value).lower() if isinstance(value, bool) else str(value)


def validate(data):
    if not isinstance(data, dict):
        raise ValueError("Experiment must be an object")
    data = copy.deepcopy(data)
    allowed = {"schema_version", "name", "target", "mode", "scale", "repetitions", "timeout_s",
               "resources", "spark_conf", "workloads"}
    if set(data) - allowed:
        raise ValueError("Unknown experiment fields: " + ", ".join(sorted(set(data) - allowed)))
    if data.get("schema_version") != 1:
        raise ValueError("schema_version must be 1")
    target = data.get("target", {})
    if not isinstance(target, dict):
        raise ValueError("target must be an object")
    adapter_info(target.get("kind"))
    create_adapter(target).validate_target()
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
                    driver_memory="1g", map_partitions=2, shuffle_partitions=2)
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
                "spark.driver.memory", "spark.sql.shuffle.partitions", "spark.default.parallelism",
                "spark.yarn.tags", "spark.app.benchmark.group.id", "spark.yarn.submit.waitAppCompletion"}
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
            if item["id"] in ("micro.wordcount", "micro.sort") and key == "datasize" and value < resources["map_partitions"]:
                raise ValueError("Logical bytes must be at least the number of generation partitions")
            if item["id"] in ("micro.terasort", "micro.repartition") and key == "datasize" and (type(value) is not int or not 1 <= value <= (2**63 - 1) // 100):
                raise ValueError("Tera dataset size must be a positive record count within the logical byte limit")
            if item["id"] == "micro.repartition" and key == "datasize" and parameters.get("fromhdfs", True) is False and value > (2**63 - 1) // resources["map_partitions"] // 200:
                raise ValueError("In-memory Repartition logical size exceeds the supported limit")
            if item["id"].startswith("sql.") and key in ("pages", "uservisits"):
                limit = (2**63 - 1) // 40 if key == "pages" else 2**63 - 1 - resources["map_partitions"] - 1
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
            if kind == "string" and (not isinstance(value, str) or not re.fullmatch(r"[\w.||-]+", value)):
                raise ValueError(f"Invalid parameter string: {key}")
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
