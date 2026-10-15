# Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from hibench import config
from hibench.adapters.runtime import properties

class SparkPropertyTests(unittest.TestCase):
    def test_advanced_properties_are_saved_and_emitted_for_submission(self):
        import yaml
        spec=config.default_experiment()
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
