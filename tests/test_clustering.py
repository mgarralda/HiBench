import json
import unittest
from pathlib import Path
from hibench import config
from hibench.catalog import workloads

class ClusteringContractTests(unittest.TestCase):
    def test_original_presets(self):
        reference=json.loads(Path('docs/ml-clustering-scale-reference.json').read_text())
        catalog={w['id']:w for w in workloads()}
        for name in ('kmeans','gmm'):
            self.assertFalse(catalog['ml.'+name]['requires_classic'])
            self.assertEqual(catalog['ml.'+name]['prepare_capabilities'], ['spark_batch'])
            for scale, values in reference[name].items():
                for key,value in values.items():
                    self.assertEqual(catalog['ml.'+name]['parameters'][key]['presets'][scale],value)

    def test_invalid_configuration(self):
        invalid=({'dimensions':0},{'num_of_samples':2**63},{'seed':1.2},
                 {'num_of_samples':3,'k':4},{'std_min':0}, {'mean_max':0},
                 {'std_min':100,'std_max':1},{'dimensions':10001,'num_of_clusters':100})
        for name in ('kmeans','gmm'):
            for parameters in invalid:
                spec=config.load('configs/smoke-clustering.yaml')
                spec['workloads']=[{'id':'ml.'+name,'parameters':parameters}]
                with self.assertRaises(ValueError): config.validate(spec)

    def test_spark_only_preparation(self):
        for name in ('kmeans','gmm'):
            path=Path(f'bin/workloads/ml/{name}/prepare/prepare.sh')
            self.assertNotIn(b'\r',path.read_bytes())
            script=path.read_text()
            self.assertIn('GaussianDataGenerator',script)
            self.assertNotIn('run_hadoop_job',script)

    def test_legacy_generator_and_dependencies_removed(self):
        self.assertFalse(Path('autogen/src/main/java/org/apache/mahout/clustering/kmeans/GenKMeansDataset.java').exists())
        for name in ('autogen/pom.xml', 'sparkbench/ml/pom.xml'):
            self.assertNotIn('org.apache.mahout',Path(name).read_text())
