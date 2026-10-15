# Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
# SPDX-License-Identifier: Apache-2.0

"""Scientific scale invariants, independent of historical default snapshots."""
import unittest

from hibench import config
from hibench.catalog import SCALES, workloads


class WorkloadScaleTests(unittest.TestCase):
    def test_every_named_scale_satisfies_workload_contracts(self):
        for item in workloads():
            for scale in SCALES:
                with self.subTest(workload=item['id'], scale=scale):
                    config.default_experiment(scale=scale, selected_workloads=[item['id']])

    def test_volume_grows_without_changing_ml_problem_width(self):
        catalog = {w['id']: w for w in workloads()}
        families = {
            **{'ml.' + name: ('examples', 'features') for name in
               ('linear', 'lr', 'svm', 'rf', 'gbt', 'pca', 'svd', 'correlation', 'summarizer')},
            'ml.kmeans': ('num_of_samples', 'dimensions'),
            'ml.gmm': ('num_of_samples', 'dimensions'),
            'ml.bayes': ('pages', 'classes'),
            'ml.lda': ('num_of_documents', 'num_of_vocabulary'),
            'ml.als': ('ratings', 'rank'),
            'websearch.pagerank': ('pages', 'num_iterations'),
            'graph.nweight': ('edges', 'degree'),
        }
        for name, (volume, width) in families.items():
            p = catalog[name]['parameters']
            values = [int(p[volume]['presets'][s]) for s in SCALES]
            with self.subTest(workload=name):
                self.assertTrue(all(a < b for a, b in zip(values, values[1:])))
                self.assertTrue(all(b == 10 * a for a, b in zip(values[1:], values[2:])))
                self.assertEqual(len({p[width]['presets'][s] for s in SCALES[1:]}), 1)

    def test_model_complexity_stays_fixed_above_tiny(self):
        fields = {'ml.pca': ('k',), 'ml.svd': ('singularvalues',),
                  'ml.lda': ('num_of_topics', 'doc_len_min', 'doc_len_max'),
                  'ml.kmeans': ('k', 'num_of_clusters', 'max_iteration', 'samples_per_inputfile'),
                  'ml.gmm': ('k', 'num_of_clusters', 'max_iteration', 'samples_per_inputfile')}
        catalog = {w['id']: w for w in workloads()}
        for name, parameters in fields.items():
            for parameter in parameters:
                with self.subTest(workload=name, parameter=parameter):
                    self.assertEqual(len({catalog[name]['parameters'][parameter]['presets'][s]
                                          for s in SCALES[1:]}), 1)
