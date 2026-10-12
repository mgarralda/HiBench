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

"""Workload reference inspection and validated experiment customization."""
import copy
import hashlib
import os
import re
import tempfile
from . import config
from .catalog import SCALES, read_properties, repository_root, workloads

MEANINGS = {
    "seed": "Random seed for reproducible data generation.",
    "noise_std": "Standard deviation of zero-mean Gaussian label noise; linear labels equal features dot coefficients plus this noise. Default: 1.0.",
    "test_fraction": "Fraction excluded from linear training (split seed 12345); reported metrics remain training metrics. Default: 0.25.",
    "class_shift": "Additive feature mean shift for class 1; alternating binary labels. Historical defaults: LR/Correlation 3.0, RF/GBT 0.3. The original row-seeded empirical profile is preserved.",
    "train_fraction": "Fraction used for model training; remaining rows are held out. Historical defaults: LR 0.6, RF/GBT 0.7.",
    "model_seed": "Seed for tree training and feature/bootstrap sampling. RF defaults to 1 for reproducibility; GBT retains its effective historical seed 0.",
    "split_seed": "Random seed for the train/test split, independently of data generation.",
    "fit_intercept": "Fit a model intercept. Logistic defaults to false to preserve the historical setting.",
    "standardization": "Standardize features during optimization; logistic defaults to true, matching historical feature scaling.",
    "threshold": "Probability threshold used for binary logistic predictions. Default: 0.5.",
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
    "numClasses": "Declared classifier class count. RF/GBT generated data remains binary; GBT requires exactly 2.",
    "ngrams": "Historical n-gram setting; the Bayes generator ignores it.",
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
    "corrType": "Feature-to-feature correlation: pearson or spearman. Labels are excluded.",
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
    "block": "Historical PageRank block-mode setting; unused by the Spark workload.",
    "block_width": "Historical PageRank block width; unused by the Spark workload.",
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
    item = next((w for w in workloads() if w["id"] == workload_id), None)
    if item is None:
        raise ValueError('Unknown workload: ' + workload_id)
    if workload_id == "websearch.pagerank":
        for name in ("block", "block_width"):
            item["parameters"][name]["note"] = "Legacy setting; not used by the current Spark launchers."
    if workload_id == "ml.bayes":
        item["parameters"]["pages"]["note"] = "Document count in text mode. The historical 479,623-position Zipf kernel requires at least 24,444 documents; all original presets satisfy this."
        item["parameters"]["class_shift"]["note"] = "Dense mode only: abs(Gaussian + original class ID times this shift). Document mode ignores this parameter."
        item["parameters"]["ngrams"]["note"] = "Historical unused property: the original generator ignored it. Preserved for reference; does not change generated features."
        item["parameters"]["smoothing"]["note"] = "Multinomial Naive Bayes additive smoothing, now passed to Spark ML. Historical effective default: 1.0."
    if workload_id == "ml.svm":
        item["parameters"]["standardization"]["note"] = "False preserves the historical SVM L2 regularization scale in original feature units; numerical optimization still changes with LinearSVC."
        item["parameters"]["tolerance"]["note"] = "LinearSVC convergence tolerance. SGD stepSize is retired; optimizer iterations and timings are not directly comparable."
    if workload_id == "ml.lda":
        item["parameters"]["seed"]["note"] = "Document seed. Changing shuffle partitions changes document streams; the historical clock seed was not reproducible."
    if workload_id == "ml.svd":
        item["parameters"]["computeU"]["note"] = "Construct the lazy distributed U plan; the historical benchmark did not materialize or write U."
        item["parameters"]["rcond"]["note"] = "Relative singular-value cutoff, with an additional Gram roundoff floor sqrt(features times machine epsilon)."
        item["parameters"]["matrix_budget_bytes"]["note"] = "Budget for estimated principal matrix arrays; JVM/native peak memory may be higher."
    if workload_id == "graph.nweight":
        item["parameters"]["edges"]["note"] = "Original pair budget: each pair emits two directed edges. Tiny: 100,000 pairs and 200,000 stored edges."
        item["parameters"]["degree"]["note"] = "Weighted path depth with immutable state; corrected version replaces historical mutable GraphX/Pregel behavior."
        item["parameters"]["seed"]["note"] = "Deterministic uniform endpoint sampling; changing shuffle partitions changes generated pairs."
    if workload_id == "ml.correlation":
        item["parameters"]["features"]["note"] = (
            "Feature count; the result requires a dense features squared matrix on the driver. "
            "Wide historical presets exceed Spark/JVM limits; preserved definitions are not execution certification.")
    if workload_id == "ml.summarizer":
        item["parameters"]["seed"]["note"] = (
            "Seed for deterministic normal(0,1) partition streams. Changing shuffle partitions changes the generated rows.")
    if workload_id == "ml.pca":
        item["parameters"]["features"]["note"] = (
            "Dense feature count. PCA fit is measured; transformation remains lazy as historically. Wide presets require solver and memory evaluation.")
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


def save_defaults(workload_id, scale, values, expected_sha256):
    """Edit the actual workload conf, preserving expressions, comments and other scales."""
    from . import store
    item, _, _, path = details(workload_id)
    if scale not in SCALES:
        raise ValueError('Unknown scale')
    original = path.read_bytes()
    if hashlib.sha256(original).hexdigest() != expected_sha256:
        raise ValueError('Configuration changed since this page was loaded. Reload before saving.')
    if store.has_active_runs():
        raise ValueError('Wait for queued or active runs to finish before changing shared configuration.')
    base = config.default_experiment()
    base['workloads'] = [{'id': workload_id, 'parameters': {}}]
    customize(base, workload_id, scale, values)
    props = read_properties(path)
    prefix = 'hibench.' + workload_id.split('.')[1] + '.'
    changes = {}
    for name, value in values.items():
        schema = item['parameters'][name]
        if value == typed_value(schema, schema['presets'][scale]):
            continue
        scaled = prefix + scale + '.' + name
        key = scaled if scaled in props else schema['property']
        if '${' in props.get(key, ''):
            raise ValueError('Edit the scale-specific value rather than replacing a derived expression: ' + key)
        changes[key] = config.scalar(value)
    if not changes:
        return {'changed': False, 'configuration_file': str(path)}
    text = original.decode('utf-8')
    for key, value in changes.items():
        pattern = re.compile(r'^(\s*' + re.escape(key) + r')\s+[^\r\n]*', re.MULTILINE)
        if not pattern.search(text):
            raise ValueError('Missing editable property: ' + key)
        text = pattern.sub(lambda match: match.group(1) + ' ' + value, text)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, suffix='.tmp', delete=False) as stream:
            temporary = stream.name
            stream.write(text.encode('utf-8'))
        if path.read_bytes() != original:
            raise ValueError('Configuration changed while saving. Reload before saving.')
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)
    return {'changed': True, 'configuration_file': str(path), 'properties': changes}
