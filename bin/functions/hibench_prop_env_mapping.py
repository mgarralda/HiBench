#!/usr/bin/env python3
# Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Mapping from properties to environment variable names
"""
HiBenchEnvPropMappingMandatory=dict(
    HIBENCH_MONITOR_DEFAULT="hibench.monitor.enabled",
    HIBENCH_MONITOR_INTERVAL_DEFAULT="hibench.monitor.interval.seconds",
    JAVA_BIN="java.bin",
    HADOOP_HOME="hibench.hadoop.home",
    HDFS_MASTER="hibench.hdfs.master",
    HADOOP_RELEASE="hibench.hadoop.release",
    HADOOP_EXECUTABLE="hibench.hadoop.executable",
    HADOOP_CONF_DIR="hibench.hadoop.configure.dir",
    HIBENCH_HOME="hibench.home",
    HIBENCH_CONF="hibench.configure.dir",

    REPORT_COLUMN_FORMATS="hibench.report.formats",
    SPARKBENCH_JAR="hibench.sparkbench.jar",
    NUM_MAPS="hibench.generator.partitions",
    NUM_REDS="hibench.generator.shuffle.partitions",
    INPUT_HDFS="hibench.workload.input",
    OUTPUT_HDFS="hibench.workload.output",
    SCALE_PROFILE="hibench.scale.profile",


    MASTERS="hibench.masters.hostnames",
    SLAVES="hibench.slaves.hostnames",
    )

HiBenchEnvPropMapping=dict(
    TEXT_SEED="hibench.text.seed",
    SPARK_HOME="hibench.spark.home",
    SPARK_MASTER="hibench.spark.master",
    SPARK_EXAMPLES_JAR="hibench.spark.examples.jar",

    HIBENCH_REPORT="hibench.report.dir", # set in default
    HIBENCH_REPORT_NAME="hibench.report.name", # set in default
    YARN_NUM_EXECUTORS="spark.executor.instances",
    YARN_EXECUTOR_CORES="spark.executor.cores",
    SPARK_YARN_EXECUTOR_MEMORY="spark.executor.memory",

    SPARK_YARN_DRIVER_MEMORY="spark.driver.memory",
    SPARK_YARN_DRIVER_CORES="spark.driver.cores",
    SPARK_YARN_DEPLOY_MODE="hibench.yarn.deploy.mode",

    DATA_HDFS="hibench.hdfs.data.dir",
    # For Sleep workload
    MAP_SLEEP_TIME="hibench.sleep.mapper.seconds",
    # For Sort, Terasort, Wordcount, Repartition
    DATASIZE="hibench.workload.datasize",
    # For repartition
    CACHE_IN_MEMORY="hibench.repartition.cacheinmemory",
    DISABLE_OUTPUT="hibench.repartition.disableoutput",
    FROM_HDFS="hibench.repartition.fromhdfs",

    # SQL workload data scale
    PAGES="hibench.workload.pages",
    USERVISITS="hibench.workload.uservisits",
    # For bayes
    CLASSES="hibench.workload.classes",
    BAYES_INPUT="hibench.bayes.dir.name.input",
    BAYES_BASE_HDFS="hibench.bayes.base.hdfs",
    NGRAMS="hibench.bayes.ngrams",
    BAYES_DENSE_SEED="hibench.bayes.dense_seed",
    BAYES_DOCUMENT_SEED="hibench.bayes.document_seed",
    BAYES_CLASS_SHIFT="hibench.bayes.class_shift",
    BAYES_SMOOTHING="hibench.bayes.smoothing",
    BAYES_TRAIN_FRACTION="hibench.bayes.train_fraction",
    BAYES_SPLIT_SEED="hibench.bayes.split_seed",
    BAYES_USE_DENSE="hibench.bayes.use_dense",
    BAYES_DENSE_EXAMPLES="hibench.bayes.dense.examples",
    BAYES_DENSE_FEATURES="hibench.bayes.dense.features",
    # For kmeans
    KMEANS_SEED="hibench.kmeans.seed",
    KMEANS_MEAN_MIN="hibench.kmeans.mean_min",
    KMEANS_MEAN_MAX="hibench.kmeans.mean_max",
    KMEANS_STD_MIN="hibench.kmeans.std_min",
    KMEANS_STD_MAX="hibench.kmeans.std_max",
    INPUT_SAMPLE="hibench.kmeans.input.sample",
    INPUT_CLUSTER="hibench.kmeans.input.cluster",
    NUM_OF_CLUSTERS="hibench.kmeans.num_of_clusters",
    NUM_OF_SAMPLES="hibench.kmeans.num_of_samples",
    SAMPLES_PER_INPUTFILE="hibench.kmeans.samples_per_inputfile",
    DIMENSIONS="hibench.kmeans.dimensions",
    MAX_ITERATION="hibench.kmeans.max_iteration",
    K="hibench.kmeans.k",
    K_STORAGE_LEVEL="hibench.kmeans.storage.level",
    K_INIT_MODE="hibench.kmeans.initializationmode",
    # For gmm
    GMM_SEED="hibench.gmm.seed",
    GMM_MEAN_MIN="hibench.gmm.mean_min",
    GMM_MEAN_MAX="hibench.gmm.mean_max",
    GMM_STD_MIN="hibench.gmm.std_min",
    GMM_STD_MAX="hibench.gmm.std_max",
    INPUT_SAMPLE_GMM="hibench.gmm.input.sample",
    INPUT_CLUSTER_GMM="hibench.gmm.input.cluster",
    NUM_OF_CLUSTERS_GMM="hibench.gmm.num_of_clusters",
    NUM_OF_SAMPLES_GMM="hibench.gmm.num_of_samples",
    SAMPLES_PER_INPUTFILE_GMM="hibench.gmm.samples_per_inputfile",
    DIMENSIONS_GMM="hibench.gmm.dimensions",
    MAX_ITERATION_GMM="hibench.gmm.max_iteration",
    K_GMM="hibench.gmm.k",
    STORAGE_LEVEL_GMM="hibench.gmm.storage.level",
    # For Logistic Regression
    NUM_PARTITIONS_LR="hibench.lr.partitions",
    SEED_LR="hibench.lr.seed",
    CLASS_SHIFT_LR="hibench.lr.class_shift",
    MAX_ITER_LR="hibench.lr.num_iterations",
    TOL_LR="hibench.lr.tolerance",
    REG_PARAM_LR="hibench.lr.regularization_param",
    FIT_INTERCEPT_LR="hibench.lr.fit_intercept",
    STANDARDIZATION_LR="hibench.lr.standardization",
    TRAIN_FRACTION_LR="hibench.lr.train_fraction",
    SPLIT_SEED_LR="hibench.lr.split_seed",
    THRESHOLD_LR="hibench.lr.threshold",
    NUM_EXAMPLES_LR="hibench.lr.examples",
    NUM_FEATURES_LR="hibench.lr.features",
    # For SVM
    PARTITIONS_SVM="hibench.svm.partitions",
    SEED_SVM="hibench.svm.seed",
    WEIGHT_SEED_SVM="hibench.svm.weight_seed",
    NOISE_STD_SVM="hibench.svm.noise_std",
    TOLERANCE_SVM="hibench.svm.tolerance",
    TRAIN_FRACTION_SVM="hibench.svm.train_fraction",
    SPLIT_SEED_SVM="hibench.svm.split_seed",
    FIT_INTERCEPT_SVM="hibench.svm.fit_intercept",
    STANDARDIZATION_SVM="hibench.svm.standardization",

    NUM_EXAMPLES_SVM="hibench.svm.examples",
    NUM_FEATURES_SVM="hibench.svm.features",
    NUM_ITERATIONS_SVM="hibench.svm.numIterations",
    SVM_STORAGE_LEVEL="hibench.svm.storage.level",
    REGPARAM_SVM="hibench.svm.regParam",
    # For ALS
    NUM_USERS_ALS="hibench.als.users",
    NUM_PRODUCTS_ALS="hibench.als.products",
    NUM_RATINGS_ALS="hibench.als.ratings",
    IMPLICITPREFS_ALS="hibench.als.implicitprefs",
    RANK_ALS="hibench.als.rank",
    NUM_ITERATIONS_ALS="hibench.als.numIterations",
    LAMBDA_ALS="hibench.als.Lambda",
    USERBLOCKS_ALS="hibench.als.numUserBlocks",
    PRODUCTBLOCKS_ALS="hibench.als.numProductBlocks",
    # For PCA
    NUM_PARTITIONS_PCA="hibench.pca.partitions",
    SEED_PCA="hibench.pca.seed",
    NUM_EXAMPLES_PCA="hibench.pca.examples",
    NUM_FEATURES_PCA="hibench.pca.features",
    PCA_K="hibench.pca.k",
    MAX_RESULT_SIZE_PCA ="hibench.pca.maxresultsize",
    # For Gradient Boosting Tree
    NUM_PARTITIONS_GBT="hibench.gbt.partitions",
    SEED_GBT="hibench.gbt.seed",
    CLASS_SHIFT_GBT="hibench.gbt.class_shift",
    MODEL_SEED_GBT="hibench.gbt.model_seed",
    SPLIT_SEED_GBT="hibench.gbt.split_seed",
    TRAIN_FRACTION_GBT="hibench.gbt.train_fraction",
    NUM_EXAMPLES_GBT="hibench.gbt.examples",
    NUM_FEATURES_GBT="hibench.gbt.features",
    NUM_CLASSES_GBT="hibench.gbt.numClasses",
    MAX_DEPTH_GBT="hibench.gbt.maxDepth",
    MAX_BINS_GBT="hibench.gbt.maxBins",
    NUM_ITERATIONS_GBT="hibench.gbt.numIterations",
    LEARNING_RATE_GBT="hibench.gbt.learningRate",
    # For XGBoost
    # For Random Forest
    NUM_PARTITIONS_RF="hibench.rf.partitions",
    SEED_RF="hibench.rf.seed",
    CLASS_SHIFT_RF="hibench.rf.class_shift",
    MODEL_SEED_RF="hibench.rf.model_seed",
    SPLIT_SEED_RF="hibench.rf.split_seed",
    TRAIN_FRACTION_RF="hibench.rf.train_fraction",
    NUM_EXAMPLES_RF="hibench.rf.examples",
    NUM_FEATURES_RF="hibench.rf.features",
    NUM_TREES_RF="hibench.rf.numTrees",
    NUM_CLASSES_RF="hibench.rf.numClasses",
    FEATURE_SUBSET_STRATEGY_RF="hibench.rf.featureSubsetStrategy",
    IMPURITY_RF="hibench.rf.impurity",
    MAX_DEPTH_RF="hibench.rf.maxDepth",
    MAX_BINS_RF="hibench.rf.maxBins",
    # For SVD
    SEED_SVD="hibench.svd.seed",
    NUM_PARTITIONS_SVD="hibench.svd.partitions",
    RCOND_SVD="hibench.svd.rcond",
    MATRIX_BUDGET_SVD="hibench.svd.matrix_budget_bytes",
    NUM_EXAMPLES_SVD="hibench.svd.examples",
    NUM_FEATURES_SVD="hibench.svd.features",
    NUM_SINGULAR_VALUES_SVD="hibench.svd.singularvalues",
    COMPUTEU_SVD="hibench.svd.computeU",
    MAXRESULTSIZE_SVD="hibench.svd.maxresultsize",
    # For Linear Regression
    NUM_EXAMPLES_LINEAR="hibench.linear.examples",
    NUM_PARTITIONS_LINEAR="hibench.linear.partitions",
    SEED_LINEAR="hibench.linear.seed",
    NOISE_STD_LINEAR="hibench.linear.noise_std",
    NUM_FEATURES_LINEAR="hibench.linear.features",
    NUM_ITERATIONS_LINEAR="hibench.linear.num_iterations",
    REG_PARAM_LINEAR="hibench.linear.regularization_param",
    ELASTICNET_PARAM_LINEAR="hibench.linear.elasticnet_param",
    TEST_FRACTION_LINEAR="hibench.linear.test_fraction",
    TOL_LINEAR="hibench.linear.tolerance",
    # For Correlation
    NUM_PARTITIONS_CORRELATION="hibench.correlation.partitions",
    SEED_CORRELATION="hibench.correlation.seed",
    CLASS_SHIFT_CORRELATION="hibench.correlation.class_shift",
    NUM_EXAMPLES_CORRELATION="hibench.correlation.examples",
    NUM_FEATURES_CORRELATION="hibench.correlation.features",
    CORR_TYPE="hibench.correlation.corrType",
    # For Summarizer
    NUM_PARTITIONS_SUMMARIZER="hibench.summarizer.partitions",
    SEED_SUMMARIZER="hibench.summarizer.seed",
    NUM_EXAMPLES_SUMMARIZER="hibench.summarizer.examples",
    NUM_FEATURES_SUMMARIZER="hibench.summarizer.features",
    # For LDA
    PARTITIONS_LDA="hibench.lda.partitions",
    SEED_LDA="hibench.lda.seed",
    MODEL_SEED_LDA="hibench.lda.model_seed",

    NUM_DOCUMENTS_LDA="hibench.lda.num_of_documents",
    NUM_VOCABULARY_LDA="hibench.lda.num_of_vocabulary",
    NUM_TOPICS_LDA="hibench.lda.num_of_topics",
    DOC_LEN_MIN_LDA="hibench.lda.doc_len_min",
    DOC_LEN_MAX_LDA="hibench.lda.doc_len_max",
    NUM_ITERATIONS_LDA="hibench.lda.num_iterations",
    OPTIMIZER_LDA="hibench.lda.optimizer",
    MAXRESULTSIZE_LDA="hibench.lda.maxresultsize",
    # For Pagerank
    PAGERANK_SEED="hibench.pagerank.seed",
    PAGERANK_BASE_HDFS="hibench.pagerank.base.hdfs",
    PAGERANK_INPUT="hibench.pagerank.dir.name.input",
    BLOCK="hibench.pagerank.block",
    NUM_ITERATIONS="hibench.pagerank.num_iterations",
    PEGASUS_JAR="hibench.pagerank.pegasus.dir",
    # For DFSIOE
    RD_NUM_OF_FILES="hibench.dfsioe.read.number_of_files",
    RD_FILE_SIZE="hibench.dfsioe.read.file_size",
    WT_NUM_OF_FILES="hibench.dfsioe.write.number_of_files",
    WT_FILE_SIZE="hibench.dfsioe.write.file_size",
    READ_ONLY="hibench.dfsioe.read_only",
    MAP_JAVA_OPTS="hibench.dfsioe.map.java_opts",
    RED_JAVA_OPTS="hibench.dfsioe.red.java_opts",
    # For NWeight
    SEED_NWEIGHT="hibench.nweight.seed",
    GENERATION_PARTITIONS_NWEIGHT="hibench.nweight.generation_partitions",
    MODEL_INPUT="hibench.nweight.model_path",
    EDGES="hibench.workload.edges",
    DEGREE="hibench.nweight.degree",
    MAX_OUT_EDGES="hibench.nweight.max_out_edges",

    )

HiBenchPropEnvMapping=dict([(v,k) for k, v in list(HiBenchEnvPropMapping.items())])
HiBenchPropEnvMappingMandatory=dict([(v,k) for k, v in list(HiBenchEnvPropMappingMandatory.items())])
