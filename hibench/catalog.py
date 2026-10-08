"""Batch catalog and parameter schemas derived from maintained workload configs."""
from pathlib import Path
import re

SCALES = ("tiny", "small", "large", "huge", "gigantic", "bigdata")
IDS = (
    "micro.sleep", "micro.sort", "micro.terasort", "micro.wordcount", "micro.repartition",
    "sql.aggregation", "sql.join", "sql.scan", "websearch.pagerank",
    "ml.bayes", "ml.kmeans", "ml.lr", "ml.als", "ml.pca", "ml.gbt", "ml.rf",
    "ml.svd", "ml.linear", "ml.lda", "ml.svm", "ml.gmm", "ml.correlation",
    "ml.summarizer", "graph.nweight",
)


def repository_root():
    import os
    return Path(os.environ.get("HIBENCH_REPOSITORY", Path(__file__).resolve().parents[1])).resolve()


def read_properties(path):
    result = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, *value = line.split(None, 1)
            result[key] = value[0] if value else ""
    return result


def workloads(root=None):
    root = root or repository_root()
    items = []
    for workload_id in IDS:
        category, name = workload_id.split(".")
        path = root / "conf" / "workloads" / category / (name + ".conf")
        props = read_properties(path)
        parameters = {}
        prefix = "hibench." + name + "."
        for key, value in props.items():
            if not key.startswith(prefix):
                continue
            suffix = key[len(prefix):]
            if suffix.split(".")[0] in SCALES or value.startswith(("/", "hdfs://")):
                continue
            if re.fullmatch(r"\$\{hibench\." + re.escape(name) + r"\.\$\{hibench.scale.profile\}\.[^}]+\}", value):
                kind = "number"
            elif value.lower() in ("true", "false"):
                kind = "boolean"
            elif re.fullmatch(r"-?\d+(?:\.\d+)?", value):
                kind = "number"
            elif "${" in value:
                continue
            else:
                kind = "string"
            parameters[suffix] = {"property": key, "kind": kind, "default": value,
                                  "presets": {scale: props.get(prefix + scale + "." + suffix, value)
                                              for scale in SCALES}}
            if category == "ml" and name in ("kmeans", "gmm") and suffix == "seed":
                parameters[suffix]["kind"] = "integer"
            if kind == "number":
                preset_values = list(parameters[suffix]["presets"].values())
                if all(x.lower() in ("true", "false") for x in preset_values):
                    parameters[suffix]["kind"] = "boolean"
                elif all(re.fullmatch(r"\d+", x) for x in preset_values):
                    parameters[suffix]["kind"] = "integer"
                elif not all(re.fullmatch(r"-?\d+(?:\.\d+)?", x) for x in preset_values):
                    parameters[suffix]["kind"] = "string"
            if parameters[suffix]["kind"] == "string":
                parameters[suffix]["presets"] = {scale: x[1:-1] if len(x) >= 2 and x[0] == x[-1] and x[0] in ("'", '"') else x
                                                  for scale, x in parameters[suffix]["presets"].items()}
        if category == "micro" and name in ("sort", "wordcount", "terasort", "repartition"):
            parameters["datasize"] = {"property": "hibench.workload.datasize", "kind": "integer",
                "default": props.get("hibench." + name + ".tiny.datasize", "32000"),
                "presets": {scale: props.get(prefix + scale + ".datasize", "32000") for scale in SCALES}}
            parameters["datasize"]["unit"] = ("records of 100 bytes; in-memory mode: 200 bytes per record per partition" if name == "repartition" else "records of 100 bytes") if name in ("terasort", "repartition") else "logical payload bytes (RandomTextWriter)"
        prepare_script = (root / "bin" / "workloads" / category / name / "prepare" / "prepare.sh").read_text(encoding="utf-8")
        prepare_capabilities = []
        modern = category in ("micro", "sql") or workload_id in ("ml.als", "ml.kmeans", "ml.gmm")
        if "run_hadoop_job" in prepare_script:
            prepare_capabilities.append("mapreduce")
        if "run_spark_job" in prepare_script:
            prepare_capabilities.extend(["spark_batch"] if modern else ["spark_batch", "classic_spark"])
        verified = modern or workload_id == "ml.kmeans"
        items.append({"id": workload_id, "category": category, "parameters": parameters,
                      "requires_input": workload_id != "micro.sleep",
                      "requires_classic": not modern,
                      "prepare_capabilities": prepare_capabilities,
                      "description": ("Small-data smoke test: Docker/YARN, Spark 3.5.9." if verified
                                      else "Experimental: smoke testing on this cluster is pending.")})
    return items


def requires_input(item):
    """In-memory Repartition and Sleep have no materialized input dataset."""
    return item["id"] != "micro.sleep" and not (
        item["id"] == "micro.repartition" and item.get("parameters", {}).get("fromhdfs", True) is False)
