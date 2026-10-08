# HiBench Next: batch workload modernization status

Audit of the upgrade checkout, 2026-10-07. This report covers benchmark algorithms and their own readers/writers, not generators. A benchmark that reads ObjectFile via SparkContext still requires classic Spark even if its training algorithm uses DataFrames.

SQL update, 2026-10-08: Scan, Aggregation and Join now use SparkSession-only Parquet readers and a Spark generator. The historical SQL/Hive findings below are superseded by [SQL modernization](sql-modernization.md); they remain as the original audit baseline.

ML update, 2026-10-08: ALS, KMeans and GMM now use typed Parquet and Dataset generation with Spark ML estimators. Their historical ObjectFile/SequenceFile findings below are superseded by [ML modernization](ml-modernization.md). The remaining migration strategy forbids an RDD execution fallback, including for SVD. Micro findings below are likewise superseded by [micro modernization](micro-modernization.md).

## Summary

24 controller workloads: 1 DataFrame end-to-end benchmark (Bayes), 3 SQL benchmarks, 9 hybrid implementations with modern kernels but classic API boundaries, and 11 RDD/GraphX implementations. This classification does not certify Spark Connect/serverless compatibility: SQL uses Hive support, legacy tables, SparkFiles and filesystem scripts; ML uses additional operations and platform-specific restrictions.

The full default build succeeded on Spark 3.5.9 / Scala 2.12 / Java 11. Runtime smoke validation exists for WordCount, Sort, Repartition, KMeans and SQL Scan only. Compiling is not equivalent to validating every algorithm. No new workload executions or migrations were performed for this audit.

## All maintained workloads

| Workload | State | Actual benchmark implementation | Next step |
|---|---|---|---|
| micro.wordcount | hybrid | DataFrame explode/groupBy/count; explicit SparkContext and IOCommon | Use SparkSession-only I/O; remove Sequence RDD fallback |
| micro.sort | hybrid | DataFrame orderBy; explicit SparkContext and IOCommon | Use SparkSession-only I/O; preserve global ordering semantics |
| micro.repartition | hybrid | DataFrame repartition; TeraInputFormat RDD input and TeraOutputFormat RDD output | Define a SQL/Parquet variant separately from fixed-width Tera compatibility |
| micro.terasort | rdd | RDD sorting and custom range partitioner; Hadoop Tera formats | Preserve legacy benchmark or define a new SQL sort with an explicit format contract |
| micro.sleep | rdd | SparkContext parallelize/map sleeping tasks | Define scheduling benchmark semantics before replacing tasks with SQL |
| sql.scan | sql | SparkSession SQL; Hive/SequenceFile tables and distributed SQL script | Modernize table/storage/catalog contract; no explicit RDD algorithm |
| sql.aggregation | sql | SparkSession SQL; shared SQL launcher | Same catalog/storage migration; runtime validation pending |
| sql.join | sql | SparkSession SQL; shared SQL launcher | Same catalog/storage migration; runtime validation pending |
| websearch.pagerank | rdd | Iterative RDD joins/reductions; cache | Choose DataFrame joins or a separately versioned graph implementation |
| ml.bayes | dataframe | spark.ml NaiveBayes and evaluator; reads Parquet | Already DataFrame throughout the benchmark; validate metrics and runtime |
| ml.kmeans | hybrid | spark.ml KMeans; sequenceFile and Mahout VectorWritable input | Replace benchmark reader with features Parquet; remove Mahout from benchmark module |
| ml.gmm | hybrid | spark.ml GaussianMixture; sequenceFile and Mahout VectorWritable | Replace benchmark reader with features Parquet; remove Mahout |
| ml.als | hybrid | spark.ml ALS; objectFile of Rating followed by toDF | Read typed user/item/rating columns; no objectFile |
| ml.pca | hybrid | spark.ml PCA; objectFile of spark.ml LabeledPoint | Read features via DataFrame; remove RDD loading |
| ml.linear | hybrid | spark.ml LinearRegression; objectFile of spark.ml LabeledPoint | Read features/label DataFrame; remove RDD loading |
| ml.correlation | hybrid | spark.ml.stat.Correlation; objectFile and DataFrame cache | Read features DataFrame; review result collection/cache restrictions separately |
| ml.lr | rdd | mllib LogisticRegressionWithLBFGS and old LabeledPoint | Migrate to spark.ml LogisticRegression; optimizer/regularization equivalence must be measured |
| ml.gbt | rdd | mllib GradientBoostedTrees and BoostingStrategy | Migrate to spark.ml GBTClassifier/GBTRegressor matching actual task |
| ml.rf | rdd | mllib RandomForest and old LabeledPoint | Migrate to spark.ml RandomForestClassifier; preserve split and feature settings |
| ml.lda | rdd | mllib LDA and distributed/local models | Migrate to spark.ml LDA with explicit optimizer and model-output semantics |
| ml.svm | rdd | mllib SVMWithSGD and BinaryClassificationMetrics | Evaluate spark.ml LinearSVC; different optimizer, not a drop-in benchmark replacement |
| ml.summarizer | rdd | mllib Statistics.colStats on RDD Vector | Migrate to spark.ml.stat.Summarizer and document requested statistics |
| ml.svd | rdd | mllib RowMatrix.computeSVD | No drop-in DataFrame migration; retain classic-only or design a separate numerical benchmark |
| graph.nweight | rdd | GraphX/Pregel NWeight; GraphImpl implementation APIs and RDD output | Classic graph workload; redesign graph implementation and preserve algorithm semantics |

## Additional legacy/optional implementations

- **ml.xgboost**: Optional profile; excluded from normal build/catalog. XGBoost 1.0.0, RDD ObjectFile bridge and spark.ml estimator. Upgrade dependency/JNI matrix and reader before enabling.
- **dal.kmeans**: Outside default build/catalog. Intel DAAL 2019.3.199, Mahout and RDD/mllib. Native legacy workload; retire or isolate as optional.
- **graph.pagerank**: Launcher and GraphXPageRank source remain outside the controller catalog. SparkContext + GraphX; classic-only.
- **micro.inmemrepartition**: ScalaInMemRepartition source remains in compiled micro module, without maintained controller entry. MemoryDataRDD/ShuffledRDD/custom internals; classic-only.
- **micro.dfsioe**: ScalaDFSIOE source remains in compiled micro module, without maintained controller entry. SparkContext and Hadoop Input/OutputFormats; classic-only.

## Libraries: algorithm versus packaging

`org.apache.spark.ml` and `org.apache.spark.mllib` both arrive in the Spark `spark-mllib` artifact. The artifact name does not establish that a benchmark uses the old RDD API. Inspect its imports and computation path instead.

The RDD-based `spark.mllib` package is in maintenance mode, not removed from Spark 3.5. Modernization should target `spark.ml`; see [Apache Spark documentation](https://spark.apache.org/docs/3.5.9/api/scala/org/apache/spark/mllib/linalg/index.html).

Seven active ML workloads still compute with the old API: logistic regression, GBT, random forest, LDA, SVM, summarizer and SVD. Six active ML workloads have modern algorithms but RDD readers: KMeans, GMM, ALS, PCA, linear regression and correlation. Bayes reads Parquet directly and uses spark.ml throughout its benchmark.

Mahout 0.9 remains in the ML module and its assembly, and the KMeans/GMM benchmark readers import VectorWritable. Removing generators from this audit does not remove that benchmark dependency. XGBoost 1.0.0 is optional/unvalidated. DAAL 2019.3.199 is outside the default build. scopt is argument parsing, not an ML implementation. Spark/Hadoop runtime dependencies are provided rather than bundled.

## Suggested implementation order

1. Finish SparkSession-only readers in WordCount/Sort and replace ML ObjectFile/SequenceFile readers with a documented features/label or user/item/rating schema. Generator changes remain a separate task; existing data needs an explicit conversion/import boundary.
2. Migrate LR/RF/GBT/Summarizer to spark.ml, with correctness and model-parameter checks. Then LDA and SVM, documenting optimizer/model changes instead of treating new runtime figures as directly comparable to old figures.
3. Decide which classic benchmarks remain supported: TeraSort fixed-width I/O, Sleep scheduling, GraphX/NWeight and numerical SVD. DataFrames alone do not provide equivalent replacements for every benchmark.
4. Remove or archive orphan sources and optional native modules only after deciding the maintained benchmark contract. Treat Databricks serverless as an independent execution/API validation target.

The SQL workloads also require their Hive/SequenceFile table and catalog setup to be modernized; absence of explicit RDD source code is insufficient for managed/serverless portability. Repartition output-disabled foreachPartition and caching are classic operations requiring separate review for Spark Connect.

Machine-readable evidence, source hashes and line references: [workload-status-2026-10-07.json](workload-status-2026-10-07.json). Older global RDD inventory includes generators and helpers and must not be used as an algorithm count.

## Subsequent implementation: text micro workloads

The initial inventory above records the pre-migration state. WordCount and Sort have subsequently been migrated to SparkSession-only readers/writers and Spark SQL generation. See [the new text contract](micro-text-contract.md); the original JSON remains a historical audit snapshot. Other workloads have not been migrated in this step.

## Subsequent implementation: complete micro family

Repartition, TeraSort and Sleep have now also been migrated. The five maintained
micro workloads contain no explicit RDD/SparkContext APIs. Tera generation runs
as a Spark Dataset and preserves the original record payload. The orphan DFSIOE
entry point and unused Spark-internal RDD helpers were removed. See the
[current micro contracts](micro-modernization.md). The preceding audit and JSON
remain historical snapshots; their classic-micro findings are superseded by
this implementation. Managed/serverless execution still requires validation.
