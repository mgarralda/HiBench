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

# HiBench Next development principles

## Simplicity first

The user's explicit development premise is to simplify HiBench Next as much as
possible: benchmarking is already complex. Apply this to implementation,
configuration, documentation, UI and API design.

- Prefer the smallest complete solution that meets the actual requirement.
- Keep one authoritative source for each setting; avoid duplicated defaults and
  require clear precedence for intentional experiment overrides.
- Reuse shared services across UI, API and CLI. Add abstractions, dependencies or
  infrastructure only when a concrete need justifies them.
- Keep common tasks easy to discover. Put advanced settings behind optional
  controls, with clear effective values and concise explanations.
- Do not add directories, profiles, options or compatibility layers merely to
  anticipate future needs. A more elaborate folder hierarchy is not a goal.
- When replacing a feature, remove obsolete paths when no supported use needs
  them, while preserving scientific evidence and recoverable run records.
- Preserve reproducibility, representative workload volume and behavior,
  validation evidence and explicit limitations. Simplicity must not hide changes
  to benchmark meaning or weaken these guarantees.
- Keep the product interface in English. Use the user's language in conversation.

For the configuration model, see `docs/configuration-model.md`. `conf/` is the
configuration base. User experiment overrides are downloaded/imported; do not
introduce a runtime recipes directory. Validation recipes belong with tests.
No backward compatibility is required during this development phase; remove
obsolete interfaces instead of adding fallback layers. Preserve historical
scientific evidence without using it as active configuration.
