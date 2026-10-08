"""Verify SQL Scan's SequenceFile copy independently, on tiny inputs only."""
import sys,csv,json
from collections import Counter
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("HiBench SQL correctness").getOrCreate()
jvm=spark.sparkContext._jvm
conf=spark.sparkContext._jsc.hadoopConfiguration()
def records(path):
    location=jvm.org.apache.hadoop.fs.Path(path)
    fs=location.getFileSystem(conf)
    result=Counter()
    for status in fs.listStatus(location):
        if not status.isFile() or status.getPath().getName().startswith(("_",".")):continue
        reader=jvm.org.apache.hadoop.io.SequenceFile.Reader(fs,status.getPath(),conf)
        try:
            assert reader.getValueClass().getName()=="org.apache.hadoop.io.Text"
            key=jvm.org.apache.hadoop.util.ReflectionUtils.newInstance(reader.getKeyClass(),conf)
            value=jvm.org.apache.hadoop.util.ReflectionUtils.newInstance(reader.getValueClass(),conf)
            while reader.next(key,value):
                row=next(csv.reader([value.toString()]))
                assert len(row)==9, "Unexpected uservisits schema"
                row[3]=float(row[3]); row[8]=int(row[8])
                result[tuple(row)]+=1
        finally:reader.close()
    return result
try:
    expected=records(sys.argv[1]); actual=records(sys.argv[2])
    assert expected==actual, "SQL Scan changed, lost or duplicated visits"
    assert expected, "Empty test dataset"
    print("HIBENCH_CORRECTNESS="+json.dumps(dict(sql_scan=True,rows=sum(actual.values()))))
finally:spark.stop()
