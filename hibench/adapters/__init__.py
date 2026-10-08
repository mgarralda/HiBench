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
    AdapterInfo("ssh", "SSH", False),
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
        raise ValueError(f"Adapter {info.label} is planned but not implemented; use docker or local")
    return info

def create_adapter(target):
    adapter_info(target.get("kind"))
    from .classic import DockerAdapter, LocalAdapter
    return {"docker": DockerAdapter, "local": LocalAdapter}[target["kind"]](target)
