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

"""Bundle the namespace for spark-submit --py-files and executor imports."""
from pathlib import Path
import sys
from zipfile import ZipFile, ZIP_DEFLATED


def package(source, destination):
    source = Path(source)
    with ZipFile(destination, 'w', ZIP_DEFLATED) as archive:
        for path in sorted(source.rglob('*.py')):
            if '__pycache__' not in path.parts:
                archive.write(path, 'pysparkbench/' + path.relative_to(source).as_posix())


if __name__ == '__main__':
    package(*sys.argv[1:])
