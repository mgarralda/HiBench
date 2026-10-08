"""Historical generator fixture only: freeze vocabularies and read tiny SequenceFiles."""
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL original reference fixture").getOrCreate()
try:
    root = "hdfs://spark-cluster-master:9000/HiBench-Validation/sql-vocab-reference-20261007-next"
    raw = spark._jvm.HiBench.RawData
    path = spark._jvm.org.apache.hadoop.fs.Path
    raw.createSearchKeys(path(root + "/search_keys"))
    raw.createUserAgents(path(root + "/user_agents"))
    raw.createCCodes(path(root + "/country_codes"))
    baseline = "hdfs://spark-cluster-master:9000/HiBench-Validation/sql-original-20261007-next/Input/tiny"
    data = {}
    for name in ("rankings", "uservisits"):
        rows = spark.sparkContext.sequenceFile(baseline + "/" + name,
               "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text").collect()
        data[name] = sorted(rows)
    with open("/tmp/sql-original-reference.json", "w", encoding="utf-8") as stream:
        json.dump(data, stream)
    print("HIBENCH_SQL_ORIGINAL_REFERENCE=" + json.dumps({name: len(rows) for name, rows in data.items()}))
finally:
    spark.stop()
