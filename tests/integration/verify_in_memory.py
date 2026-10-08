"""Verify the preserved legacy in-memory profile on small output only."""
import argparse
import json
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("output")
parser.add_argument("records_per_partition", type=int)
parser.add_argument("generation_partitions", type=int)
args = parser.parse_args()
spark = SparkSession.builder.appName("HiBench Next in-memory verification").getOrCreate()
try:
    rows = spark.read.parquet(args.output).collect()
    expected = bytes(range(200))
    assert len(rows) == args.records_per_partition * args.generation_partitions
    assert all(bytes(row.value) == expected for row in rows)
    print("HIBENCH_IN_MEMORY_VERIFIED=" + json.dumps(dict(records=len(rows),
          record_bytes=200, logical_bytes=len(rows) * 200)))
finally:
    spark.stop()
