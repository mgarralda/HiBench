"""Check actual CLI-prepared clustering inputs against the new data contract."""
import json
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.functions import vector_to_array

spark=SparkSession.builder.appName('Clustering controller input verification').config('spark.sql.shuffle.partitions','2').getOrCreate()
try:
    gen=spark._jvm.org.hibench.sparkbench.ml.GaussianDataGenerator
    profile=DataFrame(gen.profiles(spark._jsparkSession,30000,5,3,1234,0.,1000.,.01,100.),spark)
    expected=DataFrame(gen.dataset(spark._jsparkSession,profile._jdf,6000,2,1234),spark)
    # This verifier intentionally uses only the bounded tiny fixture (30,000 rows).
    expected_rows={r.id:tuple(r.features) for r in expected.select('id',vector_to_array('features').alias('features')).collect()}
    assert len(expected_rows)==30000
    def metadata_rows(frame):
        return sorted((r.clusterId,r.startId,r.samples,tuple(r.mean),tuple(r.stddev)) for r in frame.collect())
    expected_metadata=metadata_rows(profile)
    with open(sys.argv[1],encoding='utf-8') as f: paths=json.load(f)
    assert len(paths)==2
    for path in paths:
        actual=spark.read.parquet(path+'/samples').select('id',vector_to_array('features').alias('features'))
        rows=actual.collect()
        assert len(rows)==30000 and {r.id:tuple(r.features) for r in rows}==expected_rows
        metadata=spark.read.parquet(path+'/cluster')
        assert metadata_rows(metadata)==expected_metadata
        print('HIBENCH_CLUSTERING_CONTROLLER_VERIFIED='+json.dumps({'input':path,'rows':30000,'dimensions':3,'exact_cli_data_match':True}))
finally:
    spark.stop()
