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

"""Sort text lines using the shared Scala text/Parquet contract."""
from pysparkbench.common import arguments
from pysparkbench.micro.io import read_text, write_text


def parse(args):
    return arguments(args, [('input', str), ('output', str)])


def run(spark, p):
    write_text(read_text(spark, p.input).orderBy('value'), p.output)
