"""Workload reference inspection and validated experiment customization."""
import copy
from . import config
from .catalog import SCALES, read_properties, repository_root, workloads

MEANINGS = {
    "seed": "Random seed for reproducible data generation.",
    "noise_std": "Standard deviation of zero-mean Gaussian label noise; linear labels equal features dot coefficients plus this noise. Default: 1.0.",
    "test_fraction": "Fraction excluded from linear training (split seed 12345); reported metrics remain training metrics. Default: 0.25.",
    "pages": "Number of page or document candidates.",
    "uservisits": "Candidate visits; SQL drops visits to pages without incoming links.",
    "num_of_samples": "Number of generated samples.",
    "dimensions": "Number of features per sample.",
    "num_of_clusters": "Number of clusters in the generated dataset.",
    "k": "Algorithm component or cluster count (depending on workload).",
    "max_iteration": "Maximum algorithm iterations.",
    "samples_per_inputfile": "Maximum rows per generation chunk and Parquet file for modern clustering datasets; final files can be smaller.",
    "mean_min": "Lower bound for uniformly sampled Gaussian cluster means.",
    "mean_max": "Upper bound for uniformly sampled Gaussian cluster means.",
    "std_min": "Positive lower bound for uniformly sampled per-feature standard deviations.",
    "std_max": "Upper bound for uniformly sampled per-feature standard deviations.",
    "cacheinmemory": "Cache the input before measuring Repartition.",
    "disableoutput": "Execute Repartition without writing a materialized output.",
    "fromhdfs": "Read prepared data; when disabled, Repartition creates data in memory.",
    "mapper.seconds": "Sleep time per task, in seconds.",
    "storage.level": "Storage policy for cached data.",
    "storage_level": "Storage policy used by the graph workload.",
    "initializationmode": "KMeans initialization method.",
    "examples": "Number of generated training examples.",
    "features": "Number of features per example.",
    "dense.examples": "Examples generated in dense Bayes mode.",
    "dense.features": "Features per example in dense Bayes mode.",
    "use_dense": "Choose dense numeric Bayes data instead of document data.",
    "classes": "Number of generated classes.",
    "numClasses": "Number of classification labels.",
    "ngrams": "Length of word n-grams used by the document generator.",
    "users": "Number of users in generated recommendation data.",
    "products": "Number of items in generated recommendation data.",
    "ratings": "Number of generated user-item ratings.",
    "implicitprefs": "Use implicit-feedback ALS training.",
    "rank": "Number of latent factors in ALS.",
    "Lambda": "ALS regularization strength.",
    "numIterations": "Maximum algorithm iterations.",
    "num_iterations": "Maximum algorithm iterations.",
    "numTrees": "Number of trees in the ensemble.",
    "maxDepth": "Maximum decision-tree depth.",
    "maxBins": "Maximum bins used to discretize features.",
    "impurity": "Decision-tree split impurity measure.",
    "featureSubsetStrategy": "Feature subset considered for each tree split.",
    "learningRate": "Learning rate for boosting.",
    "stepSize": "Optimization step size.",
    "regParam": "Regularization strength.",
    "regularization_param": "Regularization strength.",
    "elasticnet_param": "Mix between L1 and L2 regularization.",
    "tolerance": "Convergence tolerance.",
    "optimizer": "Optimization algorithm.",
    "corrType": "Correlation measure.",
    "singularvalues": "Number of singular values to compute.",
    "computeU": "Compute the left singular vectors as part of SVD.",
    "maxresultsize": "Maximum result size collected by the driver.",
    "num_of_documents": "Number of generated documents.",
    "num_of_topics": "Number of topics fitted by LDA.",
    "num_of_vocabulary": "Vocabulary size for generated documents.",
    "doc_len_min": "Minimum generated document length.",
    "doc_len_max": "Maximum generated document length.",
    "edges": "Target number of generated graph edges.",
    "degree": "NWeight propagation depth.",
    "max_out_edges": "Maximum outgoing edges retained by NWeight.",
    "disable_kryo": "Disable Kryo serialization for NWeight.",
    "model": "NWeight execution model.",
    "block": "PageRank block-mode setting.",
    "block_width": "PageRank block width.",
}

def typed_value(schema, value):
    if schema["kind"] == "boolean":
        return str(value).lower() == "true"
    if schema["kind"] == "integer":
        return int(value)
    if schema["kind"] == "number":
        return float(value)
    return str(value)

def details(workload_id):
    item = next(w for w in workloads() if w["id"] == workload_id)
    if workload_id == "websearch.pagerank":
        for name in ("block", "block_width"):
            item["parameters"][name]["note"] = "Legacy setting; not used by the current Spark launchers."
    rows = []
    for name, schema in item["parameters"].items():
        rows.append({"Parameter": name, "Meaning / unit": schema.get("note", schema.get("unit", MEANINGS.get(name, ""))),
                     **{scale: str(schema["presets"][scale]) for scale in SCALES}})
    category, name = workload_id.split(".")
    path = repository_root() / "conf" / "workloads" / category / (name + ".conf")
    props = read_properties(path)
    represented = {s["property"] for s in item["parameters"].values()}
    prefix = "hibench." + name + "."
    other = [{"Property": key, "Configured value": value}
             for key, value in props.items() if key not in represented and not
             (key.startswith(prefix) and key[len(prefix):].split(".")[0] in SCALES)]
    return item, rows, other, path

def customize(experiment, workload_id, scale, values):
    """Keep original presets untouched; store only differences in an experiment."""
    item = next(w for w in workloads() if w["id"] == workload_id)
    if scale not in SCALES:
        raise ValueError("Unknown scale")
    unknown = set(values) - set(item["parameters"])
    if unknown:
        raise ValueError("Unknown workload parameters: " + ", ".join(sorted(unknown)))
    overrides = {key: value for key, value in values.items()
                 if value != typed_value(item["parameters"][key], item["parameters"][key]["presets"][scale])}
    result = copy.deepcopy(experiment)
    result["scale"] = scale
    selected = next((w for w in result["workloads"] if w["id"] == workload_id), None)
    if selected is None:
        result["workloads"].append({"id": workload_id, "parameters": overrides})
    selected = next(w for w in result["workloads"] if w["id"] == workload_id)
    selected["parameters"] = copy.deepcopy(values)
    validated = config.validate(result)
    next(w for w in validated["workloads"] if w["id"] == workload_id)["parameters"] = overrides
    return validated
