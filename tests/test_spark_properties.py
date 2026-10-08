import unittest
from hibench import config
from hibench.backend import properties

class SparkPropertyTests(unittest.TestCase):
    def test_advanced_properties_are_saved_and_emitted_for_submission(self):
        import yaml
        spec=config.load("configs/docker-yarn.yaml")
        settings=yaml.safe_load("""spark.dynamicAllocation.enabled: true
spark.dynamicAllocation.shuffleTracking.enabled: true
spark.dynamicAllocation.minExecutors: 2
spark.dynamicAllocation.maxExecutors: 10
spark.sql.adaptive.enabled: true
spark.sql.adaptive.coalescePartitions.enabled: true
spark.sql.adaptive.advisoryPartitionSizeInBytes: 64m
spark.executor.extraJavaOptions: '-XX:+UseG1GC -Dexample=true'
""")
        spec["spark_conf"]=settings
        saved=config.validate(yaml.safe_load(yaml.safe_dump(config.validate(spec))))
        self.assertEqual(saved["spark_conf"],settings)
        emitted=properties(saved,saved["workloads"][0],"test","/data","/report","/output")
        for key,value in settings.items():
            self.assertIn(key+" "+config.scalar(value)+"\n",emitted)
