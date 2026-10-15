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

"""Small shared argument and validation helpers; no workload implementations."""
import argparse
import math
from pyspark import StorageLevel


def boolean(value):
    if value.lower() not in ('true', 'false'):
        raise argparse.ArgumentTypeError('Expected true or false')
    return value.lower() == 'true'


def arguments(args, positional, options=()):
    parser = argparse.ArgumentParser()
    for name, kind in positional:
        parser.add_argument(name, type=kind)
    for name, kind, default in options:
        flags = ['--' + name]
        if name == 'k':
            flags.insert(0, '-k')
        parser.add_argument(*flags, type=kind, default=default)
    return parser.parse_args(args)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def finite(value, message):
    require(math.isfinite(value), message)
    return value


def storage_level(name):
    # Scala's *_ONLY / *_AND_DISK constants store deserialized values by default.
    levels = {
        'NONE': StorageLevel(False, False, False, False),
        'DISK_ONLY': StorageLevel(True, False, False, False),
        'DISK_ONLY_2': StorageLevel(True, False, False, False, 2),
        'DISK_ONLY_3': StorageLevel(True, False, False, False, 3),
        'MEMORY_ONLY': StorageLevel(False, True, False, True),
        'MEMORY_ONLY_2': StorageLevel(False, True, False, True, 2),
        'MEMORY_ONLY_SER': StorageLevel(False, True, False, False),
        'MEMORY_ONLY_SER_2': StorageLevel(False, True, False, False, 2),
        'MEMORY_AND_DISK': StorageLevel(True, True, False, True),
        'MEMORY_AND_DISK_2': StorageLevel(True, True, False, True, 2),
        'MEMORY_AND_DISK_SER': StorageLevel(True, True, False, False),
        'MEMORY_AND_DISK_SER_2': StorageLevel(True, True, False, False, 2),
        'OFF_HEAP': StorageLevel(True, True, True, False),
    }
    require(name in levels, 'Unknown storage level: ' + name)
    return levels[name]
