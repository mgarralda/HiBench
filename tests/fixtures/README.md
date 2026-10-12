<!--
Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Test references

`contracts/` contains byte-preserving copies of the historical scale/profile references previously read from Git-ignored documentation. Tests use these committed fixtures so source distributions and fresh CI checkouts do not depend on local documentation. Documentation copies remain available as explanatory artifacts.

`acceptance/references/` contains the frozen migration ledger, original contracts and referenced receipt files needed for evidence-attribution tests; `acceptance/conf/` retains the configuration baseline. Tests reconstruct the expected documentation paths in a temporary directory. The references directory avoids the repository's global `docs/` ignore rule. This is a historical fixture, not a claim about the current runtime/campaign. Tests continue to reject shared-success shortcuts and custom-size certification of named presets. No expected data values were changed in relocating these files.
