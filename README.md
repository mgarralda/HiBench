# HiBench Batch — Spark 3.5.9

Build/runtime reference: Java 11, Spark 3.5.9, Scala 2.12.18, Python 3.10+. Default build is batch only. This community fork is not an official Intel release.

HiBench Next uses `org.hibench` for the project packages formerly under Intel's namespace. See the [namespace migration guide](docs/namespace-migration.md) before using custom class names or older JARs.

All five `micro.*` workloads and their preparation paths now use SparkSession and DataFrames/Datasets, without MapReduce or RDD boundaries. See the [micro contracts and migration notes](docs/micro-modernization.md); `configs/smoke-micro.yaml` exercises the complete micro family.

The three `sql.*` workloads now use Spark SQL over Parquet and a Spark generator, without a Hive metastore or MapReduce preparation. See the [SQL contracts and validation](docs/sql-modernization.md); `configs/smoke-sql.yaml` exercises the SQL family.

ML migration covers `ml.als`, `ml.kmeans` and `ml.gmm`: Spark ML estimators, typed Parquet inputs and Dataset generation, with no application RDD boundary. See [ML modernization](docs/ml-modernization.md); `configs/smoke-als.yaml` and `configs/smoke-clustering.yaml` exercise preparation and training. Other ML workloads are still being migrated.

## New controller and Streamlit UI

Build with JDK 11 and Maven 3.9. On Windows, `./bin/build-java11.ps1` builds and tests inside the Maven/Java 11 Docker image, so the host's old Java installation can remain in place.

```sh
mvn -B -DskipTests clean package
python -m venv .venv
# Activate the virtual environment before the following commands.
python -m pip install -e ".[web]"
hibench validate configs/docker-yarn.yaml
hibench install configs/docker-yarn.yaml
hibench doctor configs/docker-yarn.yaml
hibench run configs/docker-yarn.yaml --wait
python -m streamlit run hibench/web.py --server.fileWatcherType none
```

The sample prepares and runs tiny WordCount and Sort on Docker/YARN. Reinstall after changing runtime scripts or JARs. The UI shares the CLI schema and offers parameters, Run, logs, results, cancellation and durable history. See [controller guide](docs/controlador-batch.md), [verified results](docs/upgrade-validation-2026-10-07.md), [audit](docs/compatibilidad-spark-3.5.md) and [platform analysis](docs/plataformas-y-control-streamlit.md).

MapReduce preparation, Mahout types and classic Spark boundaries remain. Databricks serverless compatibility is not achieved. Cloud submission/storage adapters are not implemented. The 24 batch catalog entries are candidates; compilation does not certify every workload. XGBoost's historical dependencies remain in an unvalidated optional `xgboost` profile.

Python checks: `python -m unittest discover -s tests -v`. Metrics now use named schema-2 CSV fields. Elapsed time includes submission and startup. Legacy monitoring is disabled by default.

## Historical Spark 3.3 notes

The following describes the previous version; use the commands and versions above for the upgrade branch.

# HiBench for Apache Spark 3.3 (Scala-based)

This repository provides an updated version of [Intel's HiBench](https://github.com/Intel-bigdata/HiBench), adapted to support **Apache Spark 3.3.x**, **Scala 2.12**, and modern DataFrame APIs.

> ⚠️ This repository provides a research-oriented extension of Intel HiBench for modern Spark environments. It is not an official release.

---

## 🧭 Scope of This Version

- Targeted for **local Spark + Spark Hadoop clusters**
- Validated in **Azure HDInsight HDI 5.1** with **Azure Blob Storage**
- Designed to run on **ephemeral Spark clusters** while keeping input/output out of the cluster

---

## 🔧 Our Improvements

- ✅ **Spark 3.3.x** and **Hadoop 3.2.x**, and **Scala 2.12.x** with updated build scripts and dependencies
- ✅ Enabled support for **YARN cluster deploy mode**, keeping the original **local** and **client** modes
- ✅ Several workloads refactored to use **DataFrame API** instead of legacy RDD
- ✅ Provided **WABS** compatibility: allows the complex task of mapping file system semantics (as required by the HDFS) to object store style interface exposed by Azure Blob Storage.
- ✅ Designed to support **ephemeral clusters** with persistent I/O data generated out the cluster
- ✅ Added support for only one generation of **specific input data** for each workload
- ✅ Added **Docker** support for local testing and development
- ✅ Enhanced the **hibench.report** to show the application_id and input data size scale.
---

### Spark Cluster based on docker ###
You can use our Docker Spark Cluster version for development and testing.
Link: [Docker Spark Cluster](https://github.com/mgarralda/hadoop-spark-cluster)


## 📚 Original Work Attribution

This project is based on the original **HiBench** suite by:
- **Intel Corporation**
- Repository: [https://github.com/Intel-bigdata/HiBench](https://github.com/Intel-bigdata/HiBench)

Modifications by:
- **Mariano Garralda (2025)**
- University of A Coruña (UDC)

See the [NOTICE](NOTICE) file for details.

---

## 📜 License

This project is licensed under the [Apache License 2.0](LICENSE).  
See the [NOTICE](NOTICE) file for attribution.

---

## 📄 Citation

If you use this work, the benchmarking environment, workloads, datasets, experimental infrastructure, or derived components in research, benchmarking studies, technical documentation, or derivative works, please cite the associated publications and/or doctoral thesis.

### References

- Garralda-Barrio, M., Eiras-Franco, C., & Bolón-Canedo, V. (2024).  
  *A novel framework for generic Spark workload characterization and similar pattern recognition using machine learning*.  
  *Journal of Parallel and Distributed Computing*, 189, 104881.  
  https://doi.org/10.1016/j.jpdc.2024.104881

- Garralda Barrio, M. (2026).  
  *AI-Driven Optimization in Distributed Computing Systems: A Self-Tuning Framework*.  
  Doctoral Thesis, University of Coruña.  
  https://hdl.handle.net/2183/48114

<details>
    <summary>📚 Citation (BibTeX)</summary>

```bibtex
@article{garralda2024novel,
  title        = {A novel framework for generic Spark workload characterization and similar pattern recognition using machine learning},
  author       = {Garralda-Barrio, Mariano and Eiras-Franco, Carlos and Bol{\'o}n-Canedo, Ver{\'o}nica},
  journal      = {Journal of Parallel and Distributed Computing},
  volume       = {189},
  pages        = {104881},
  year         = {2024},
  doi          = {10.1016/j.jpdc.2024.104881},
  publisher    = {Elsevier}
}

@phdthesis{GarraldaBarrio2026,
  author       = {Mariano Garralda Barrio},
  title        = {AI-Driven Optimization in Distributed Computing Systems: A Self-Tuning Framework},
  school       = {University of Coruña},
  year         = {2026},
  type         = {Doctoral Thesis},
  url          = {https://hdl.handle.net/2183/48114}
}
```

</details>

## 🐳 Spark Cluster for Local Testing

You can use our Docker-based Spark-Hadoop cluster for development and validation:

🔗 [Docker Spark Cluster](https://github.com/mgarralda/hadoop-spark-cluster)

---

*This README was adapted and extended to reflect the specific contributions made in the context of a research benchmark study targeting Spark 3.3 compatibility and cloud deployment support.*


# HiBench Suite
## The bigdata micro benchmark suite ##


* Current version: 1.0
* Homepage: https://github.com/intel-hadoop/HiBench
* Contents:
  1. Overview
  2. Getting Started
  3. Workloads
  4. Supported Releases

---
## 📦 HiBench Suite Overview

HiBench is a big data benchmark suite that helps evaluate Spark workloads in terms of speed, throughput and system resource utilizations.


### Workloads ###

There are totally 24 workloads in HiBench. The workloads are divided into 6 categories which are micro, ml(machine learning), sql, graph, and websearch.

  **Micro Benchmarks:**

1. Sort (sort)

    This workload sorts its *text* input data, which is generated using RandomTextWriter.

2. WordCount (wordcount)

    This workload counts the occurrence of each word in the input data, which are generated using RandomTextWriter. It is representative of another typical class of real world MapReduce jobs - extracting a small amount of interesting data from large data set.

3. TeraSort (terasort)

    TeraSort is a standard benchmark created by Jim Gray. Its input data is generated by Hadoop TeraGen example program.
    
4. Repartition (micro/repartition)
    
    This workload benchmarks shuffle performance. Input data is generated by Hadoop TeraGen. The workload randomly selects the post-shuffle partition for each record, performs shuffle write and read, evenly repartitioning the records. There are 2 parameters providing options to eliminate data source & sink I/Os: hibench.repartition.cacheinmemory(default: false) and hibench.repartition.disableOutput(default: false), controlling whether or not to 1) cache the input in memory at first 2) write the result to storage

5. Sleep (sleep)

    This workload sleep an amount of seconds in each task to test framework scheduler.


**Machine Learning:**

1. Bayesian Classification (Bayes)

    Naive Bayes is a simple multiclass classification algorithm with the assumption of independence between every pair of features. This workload is implemented in spark.mllib and uses the automatically generated documents whose words follow the zipfian distribution. The dict used for text generation is also from the default linux file /usr/share/dict/linux.words.ords.

2. K-means clustering (Kmeans)

    This workload uses Spark ML KMeans over Parquet vectors. Its Dataset generator preserves balanced Gaussian components, the historical mean and standard-deviation ranges and all original size presets, with reproducible seeds. See [ML modernization](docs/ml-modernization.md).
    
3. Gaussian Mixture Model (GMM) 

    Gaussian Mixture Model represents a composite distribution whereby points are drawn from one of k Gaussian sub-distributions, each with its own probability. It's implemented in spark.mllib. The input data set is generated by GenKMeansDataset based on Uniform Distribution and Guassian Distribution.    

4. Logistic Regression (LR)

    Logistic Regression (LR) is a popular method to predict a categorical response. This workload is implemented in spark.mllib with LBFGS optimizer and the input data set is generated by LogisticRegressionDataGenerator based on random balance decision tree. It contains three different kinds of data types, including categorical data, continuous data, and binary data.

5. Alternating Least Squares (ALS)

    The alternating least squares (ALS) algorithm is a well-known algorithm for collaborative filtering. This workload is implemented in spark.mllib and the input data set is generated by RatingDataGenerator for a product recommendation system.

6. Gradient Boosted Trees (GBT)

    Gradient-boosted trees (GBT) is a popular regression method using ensembles of decision trees. This workload is implemented in spark.mllib and the input data set is generated by GradientBoostedTreeDataGenerator.

7. XGBoost (XGBoost)

    XGBoost is an optimized distributed gradient boosting library designed to be highly efficient, flexible and portable. This workload is implemented with XGBoost4J-Spark API in spark.mllib and the input data set is generated by GradientBoostedTreeDataGenerator.    

8. Linear Regression (Linear)

    Linear Regression uses Spark ML with ElasticNet and a SparkSession/Dataset
    generator. Dense features and continuous labels are stored in Parquet; original
    size presets and the noisy linear model are preserved. See the
    [data and algorithm contract](docs/ml-linear-modernization.md).

9. Latent Dirichlet Allocation (LDA)

    Latent Dirichlet allocation (LDA) is a topic model which infers topics from a collection of text documents. This workload is implemented in spark.mllib and the input data set is generated by LDADataGenerator.

10. Principal Components Analysis (PCA)

    Principal component analysis (PCA) is a statistical method to find a rotation such that the first coordinate has the largest variance possible, and each succeeding coordinate in turn has the largest variance possible. PCA is used widely in dimensionality reduction. This workload is implemented in spark.ml. The input data set is generated by PCADataGenerator.

11. Random Forest (RF)

    Random forests (RF) are ensembles of decision trees. Random forests are one of the most successful machine learning models for classification and regression. They combine many decision trees in order to reduce the risk of overfitting. This workload is implemented in spark.mllib and the input data set is generated by RandomForestDataGenerator.

12. Support Vector Machine (SVM)

    Support Vector Machine (SVM) is a standard method for large-scale classification tasks. This workload is implemented in spark.mllib and the input data set is generated by SVMDataGenerator.

13. Singular Value Decomposition (SVD)

    Singular value decomposition (SVD) factorizes a matrix into three matrices. This workload is implemented in spark.mllib and its input data set is generated by SVDDataGenerator.


**SQL:**

1. Scan (scan) 2. Join (join), 3. Aggregate (aggregation)

    These workloads originate from the SIGMOD 09 paper "A Comparison of Approaches to Large-Scale Data Analysis" and HIVE-396. HiBench Next runs Scan, Aggregation and Join using Spark SQL over Parquet, without Hive tables or a metastore. The Spark generator preserves the original scale presets and representative web-data profiles, including Zipfian hyperlink popularity. See [SQL modernization](docs/sql-modernization.md) for schemas, query semantics and validation.

**Websearch Benchmarks:**

1. PageRank (pagerank)

    This workload benchmarks PageRank algorithm implemented in Spark-MLLib/Hadoop (a search engine ranking benchmark included in pegasus 2.0) examples. The data source is generated from Web data whose hyperlinks follow the Zipfian distribution.

rge-scale search indexing is one of the most significant uses of MapReduce. This workload tests the indexing sub-system in Nutch, a popular open source (Apache project) search engine. The workload uses the automatically generated Web data whose hyperlinks and words both follow the Zipfian distribution with corresponding parameters. The dict used to generate the Web page texts is the default linux dict file.

**Graph Benchmark:**

1. NWeight (nweight) 

    NWeight is an iterative graph-parallel algorithm implemented by Spark GraphX and pregel. The algorithm computes associations between two vertices that are n-hop away. 


### Supported Hadoop/Spark releases: ###

  - Hadoop: Apache Hadoop 3.3.x
  - Spark: Spark 3.3.x

---

## Migration evidence and reproducibility

See the [migration evidence index](docs/reproducibility/README.md) for generator and algorithm changes, validation limits, preserved test logs, and recoverable source snapshots.
