# Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
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

"""Infrastructure profiles and explicit precedence, shared by every interface."""
import copy
import json
import re
import time
from hibench import config, store

SECTIONS = ('target', 'resources', 'spark_conf')


def initialize():
    with store.connect() as db:
        db.execute('CREATE TABLE IF NOT EXISTS profiles (name TEXT PRIMARY KEY, data TEXT NOT NULL, updated REAL NOT NULL)')


def profiles():
    initialize()
    with store.connect() as db:
        return [{'name': r['name'], 'settings': json.loads(r['data']), 'updated': r['updated']}
                for r in db.execute('SELECT * FROM profiles ORDER BY name')]


def get_profile(name):
    for profile in profiles():
        if profile['name'] == name:
            return profile
    raise KeyError('Unknown infrastructure profile')


def save_profile(name, settings):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_. -]{0,79}', name):
        raise ValueError('Profile name must contain 1–80 letters, digits, spaces, dots, underscores or hyphens')
    if not isinstance(settings, dict) or set(settings) - set(SECTIONS):
        raise ValueError('Infrastructure profiles contain target, resources and spark_conf only')
    from hibench.catalog import repository_root
    base = config.default_experiment()
    for key, value in settings.items():
        if not isinstance(value, dict):
            raise ValueError(key + ' must be an object')
        base[key] = {**base[key], **value}
    validated = config.validate(base)
    normalized = {key: validated[key] for key in SECTIONS}
    initialize()
    with store.connect() as db:
        db.execute('INSERT INTO profiles VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET data=excluded.data, updated=excluded.updated',
                   (name, json.dumps(normalized, sort_keys=True), time.time()))
    return get_profile(name)


def delete_profile(name):
    get_profile(name)
    with store.connect() as db:
        db.execute('DELETE FROM profiles WHERE name=?', (name,))


def resolve(experiment, profile=None):
    """Profile is a base; explicit experiment keys override it; validation fills defaults."""
    result = config.default_experiment(scale=experiment.get('scale'))
    for key, value in (profile or {}).items():
        result[key] = {**result.get(key, {}), **copy.deepcopy(value)} if key in SECTIONS else copy.deepcopy(value)
    for key, value in experiment.items():
        if key in SECTIONS:
            if not isinstance(value, dict):
                raise ValueError(key + ' must be an object')
            result[key] = {**result.get(key, {}), **copy.deepcopy(value)}
        else:
            result[key] = copy.deepcopy(value)
    return config.validate(result)


def apply_profile(experiment, settings):
    """UI selection deliberately replaces infrastructure; workload selection is retained."""
    result = copy.deepcopy(experiment)
    result.update(copy.deepcopy(settings))
    return config.validate(result)
