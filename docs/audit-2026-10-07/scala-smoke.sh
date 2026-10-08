#!/usr/bin/env bash
set -euo pipefail
audit_root=/tmp/hibench-audit-20261007
mkdir -p "$audit_root/input"
printf 'a b a\nb c\n' > "$audit_root/input/data.txt"
printf 'sparkbench.inputformat Text\nsparkbench.outputformat Text\n' > "$audit_root/sparkbench.conf"
export SPARKBENCH_PROPERTIES_FILES="$audit_root/sparkbench.conf"
common="$audit_root/sparkbench-common.jar"
micro="$audit_root/sparkbench-micro.jar"
spark-submit --master 'local[2]' --conf spark.eventLog.enabled=false --conf spark.sql.shuffle.partitions=2 --jars "$common" --class com.intel.hibench.sparkbench.micro.ScalaWordCount "$micro" "file://$audit_root/input" "file://$audit_root/wordcount-output"
spark-submit --master 'local[2]' --conf spark.eventLog.enabled=false --conf spark.sql.shuffle.partitions=2 --jars "$common" --class com.intel.hibench.sparkbench.micro.ScalaSort "$micro" "file://$audit_root/input" "file://$audit_root/sort-output"
hdfs_root=/tmp/hibench-audit-20261007
hdfs dfs -mkdir -p "$hdfs_root/input"
hdfs dfs -put "$audit_root/input/data.txt" "$hdfs_root/input/data.txt"
spark-submit --master yarn --deploy-mode client --num-executors 1 --executor-cores 1 --executor-memory 1g --driver-memory 1g --conf spark.sql.shuffle.partitions=2 --jars "$common" --class com.intel.hibench.sparkbench.micro.ScalaWordCount "$micro" "hdfs://spark-cluster-master:9000$hdfs_root/input" "hdfs://spark-cluster-master:9000$hdfs_root/wordcount-output"
echo HIBENCH_SCALA_SMOKE_OK
