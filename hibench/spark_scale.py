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

"""Pure scale resolution shared by the controller and installed shell loader."""
SCALES = ('tiny', 'small', 'large', 'huge', 'gigantic', 'bigdata')
PREFIX = '__scale__.'


def resolve_scale(properties, scale):
    if scale not in SCALES:
        raise ValueError('Unknown Spark scale: ' + str(scale))
    values = {k: v for k, v in properties.items() if not k.startswith(PREFIX)}
    sources = {k: 'common' for k in values}
    for key, value in properties.items():
        if not key.startswith(PREFIX):
            continue
        size, separator, property_name = key[len(PREFIX):].partition('.')
        if size not in SCALES or not separator or not property_name.startswith('spark.'):
            raise ValueError('Invalid scale property: ' + key)
        if property_name in {'spark.master', 'spark.submit.deployMode', 'spark.app.name', 'spark.extraListeners', 'spark.yarn.tags', 'spark.yarn.submit.waitAppCompletion'} or property_name.startswith('spark.app.benchmark.'):
            raise ValueError('Scale configuration cannot change target or launcher identifiers: ' + key)
        if size == scale:
            values[property_name] = value
            sources[property_name] = 'scale:' + scale
    return values, sources


def parse_spark(text):
    """Native Spark keys grouped by optional benchmark scale sections."""
    section = 'common'
    seen_sections = set()
    values = {}
    for number, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('['):
            if not line.endswith(']') or line[1:-1] not in ('common', *SCALES):
                raise ValueError('Invalid Spark section on line ' + str(number))
            section = line[1:-1]
            if section in seen_sections:
                raise ValueError('Duplicate Spark section: ' + section)
            seen_sections.add(section)
            continue
        key, *value = line.split(None, 1)
        import re
        if not re.fullmatch(r'spark\.[\w.]+', key):
            raise ValueError('Invalid Spark property: ' + key)
        stored = key if section == 'common' else PREFIX + section + '.' + key
        if stored in values:
            raise ValueError('Duplicate Spark property in ' + section + ': ' + key)
        values[stored] = value[0] if value else ''
    resolve_scale(values, 'small')
    return values
