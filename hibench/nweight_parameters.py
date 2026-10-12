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

"""Original pair-count profiles and explicit modern recurrence controls."""
def validate_nweight(parameters,schema,scale,resources):
    for key,maximum in [('edges',4250000000),('degree',2**31-1),('max_out_edges',2**31-1),('seed',2**63-2)]:
        value=parameters.get(key,int(schema[key]['presets'][scale]))
        if type(value) is not int or not (0 if key=='seed' else 1)<=value<=maximum:
            raise ValueError('Invalid NWeight '+key)
    count=parameters.get('edges',int(schema['edges']['presets'][scale]))
    partitions=resources['generation_shuffle_partitions']
    if (count*6//5+partitions-1)//partitions>=2**31-1:
        raise ValueError('NWeight generation needs more generator shuffle partitions for this pair budget')
