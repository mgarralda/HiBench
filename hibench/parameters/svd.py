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

"""Numerical and memory contracts for the packed Gram SVD workload."""
import math

def validate_svd(parameters, schema, scale):
    values={key:parameters.get(key,int(schema[key]['presets'][scale]))
            for key in ('examples','features','singularvalues','seed','matrix_budget_bytes')}
    for key,value in values.items():
        limit=2**63-1 if key in ('examples','seed','matrix_budget_bytes') else 2**31-1
        if type(value) is not int or not (0 if key=='seed' else 1)<=value<=limit:
            raise ValueError('Invalid SVD '+key)
    d=values['features'];k=values['singularvalues']
    if k>d or d*d>2**31-1:
        raise ValueError('SVD component count or matrix dimension exceeds supported range')
    estimated=8*(3*d*d+d*k+d*(d+1)//2)
    if estimated>values['matrix_budget_bytes']:
        raise ValueError('SVD matrix array estimate exceeds configured matrix budget')
    cutoff=parameters.get('rcond',float(schema['rcond']['default']))
    if not math.isfinite(cutoff) or not 0<cutoff<1:
        raise ValueError('SVD rcond must be finite and in (0, 1)')
