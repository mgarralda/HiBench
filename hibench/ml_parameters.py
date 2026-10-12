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

"""Contracts for the final ML migration candidates, separate from runtime certification."""
import math

def validate_pending_ml(workload_id, parameters, schema, scale, resources):
    if workload_id not in ("ml.bayes", "ml.svm", "ml.lda"):
        return
    family=workload_id.split('.')[1]
    keys={"bayes":("pages","dense.examples","dense.features","classes","dense_seed","document_seed","split_seed"),
          "svm":("examples","features","numIterations","seed","weight_seed","split_seed"),
          "lda":("num_of_documents","num_of_vocabulary","num_of_topics","doc_len_min","doc_len_max","num_iterations","seed","model_seed")}[family]
    values={key:parameters.get(key,int(schema[key]['presets'][scale])) for key in keys}
    for key,value in values.items():
        is_seed='seed' in key
        limit=2**63-1 if is_seed or key in ('pages','examples','dense.examples','num_of_documents') else 2**31-1
        minimum=0 if is_seed else (2 if key in ('classes','num_of_topics') else 1)
        if type(value) is not int or not minimum<=value<=limit:
            raise ValueError(f"Invalid {family} {key}: expected integer in [{minimum}, {limit}]")
    if family in ('bayes','svm'):
        fraction=parameters.get('train_fraction',float(schema['train_fraction']['default']))
        if not 0<fraction<1:
            raise ValueError('Training fraction must be in (0, 1)')
    if family=='bayes':
        dense=parameters.get('use_dense',schema['use_dense']['presets'][scale].lower()=='true')
        if dense and values['dense_seed']+values['dense.examples']-1>2**63-1:
            raise ValueError('Dense Bayes row seed exceeds Long range')
        if not dense and (values['pages']<24444 or values['pages']>(2**63-1)//760 or values['document_seed']<1 or values['document_seed']+resources['generation_partitions']-1>2**31-1):
            raise ValueError('Document Bayes page budget or mapper seed exceeds supported range')
    if family=='svm':
        if values['seed']+values['examples']-1>2**63-1:
            raise ValueError('SVM row seed exceeds Long range')
        tolerance=parameters.get('tolerance',float(schema['tolerance']['default']))
        if not math.isfinite(tolerance) or tolerance<=0:
            raise ValueError('SVM tolerance must be positive and finite')
    if family=='lda':
        if values['doc_len_max']<values['doc_len_min'] or values['doc_len_max']-values['doc_len_min']+1>2**31-1:
            raise ValueError('Invalid LDA document length range')
        if parameters.get('optimizer',schema['optimizer']['default']) not in ('online','em'):
            raise ValueError('LDA optimizer must be online or em')
    for key in ('class_shift','smoothing','noise_std','regParam'):
        if key in schema:
            value=parameters.get(key,float(schema[key]['default']))
            if not math.isfinite(value) or value<0:
                raise ValueError(f'{key} must be finite and nonnegative')
