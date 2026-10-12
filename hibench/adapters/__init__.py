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

"""Adapter registry. Planned destinations cannot be selected for execution."""
from dataclasses import dataclass
from .base import ExecutionAdapter, JobReference, PhaseJob

@dataclass(frozen=True)
class AdapterInfo:
    kind: str
    label: str
    implemented: bool

REGISTRY = (
    AdapterInfo("docker", "Existing Docker", True),
    AdapterInfo("local", "Local Linux", True),
    AdapterInfo("ssh", "SSH", True),
    AdapterInfo("livy", "Apache Livy", False),
    AdapterInfo("emr", "AWS EMR", False),
    AdapterInfo("dataproc", "Google Dataproc", False),
    AdapterInfo("hdinsight", "Azure HDInsight", False),
    AdapterInfo("synapse", "Azure Synapse", False),
    AdapterInfo("fabric", "Microsoft Fabric", False),
    AdapterInfo("databricks", "Databricks", False),
)

def adapter_info(kind):
    info = next((entry for entry in REGISTRY if entry.kind == kind), None)
    if info is None:
        raise ValueError(f"Unknown execution adapter: {kind}")
    if not info.implemented:
        raise ValueError(f"Adapter {info.label} is planned but not implemented; use docker, ssh or local")
    return info

def create_adapter(target):
    adapter_info(target.get("kind"))
    from .classic import DockerAdapter, LocalAdapter
    from .ssh import SSHAdapter
    return {"docker": DockerAdapter, "local": LocalAdapter, "ssh": SSHAdapter}[target["kind"]](target)
