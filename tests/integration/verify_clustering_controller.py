"""Check actual CLI-prepared clustering inputs against the new data contract."""
import json
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.functions import vector_to_array

spark=SparkSession.builder.appName('Clustering controller input verification').getOrCreate()
try:
    gen=spark._jvm.org.hibench.sparkbench.ml.GaussianDataGenerator
    profile=DataFrame(gen.profiles(spark._jsparkSession,30000,5,3,1234,0.,1000.,.01,100.),spark)
    expected=DataFrame(gen.dataset(spark._jsparkSession,profile._jdf,6000,2,1234),spark)
    expected=expected.select('id',vector_to_array('features').alias('features')).cache()
    with open(sys.argv[1],encoding='utf-8') as f: paths=json.load(f)
    assert len(paths)==2
    for path in paths:
        actual=spark.read.parquet(path+'/samples').select('id',vector_to_array('features').alias('features'))
        assert actual.count()==30000
        assert actual.exceptAll(expected).count()==0 and expected.exceptAll(actual).count()==0
        metadata=spark.read.parquet(path+'/cluster')
        assert metadata.exceptAll(profile).count()==0 and profile.exceptAll(metadata).count()==0
        print('HIBENCH_CLUSTERING_CONTROLLER_VERIFIED='+json.dumps({'input':path,'rows':30000,'dimensions':3,'exact_cli_data_match':True}))
    expected.unpersist()
finally:
    spark.stop()
