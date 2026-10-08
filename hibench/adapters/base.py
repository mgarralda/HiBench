"""Provider-neutral execution contract; preparation and benchmarking are separate jobs."""
import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

@dataclass(frozen=True)
class PhaseJob:
    run_id: str
    spec: dict
    workload: dict
    phase: str
    index: int
    dataset_root: str
    directory: Path

@dataclass
class JobReference:
    adapter: str
    run_id: str
    phase: str
    native_id: str
    metadata: dict = field(default_factory=dict)
    # Runtime handles are deliberately excluded from persisted snapshots.
    runtime: object = field(default=None, repr=False)

    def snapshot(self):
        return dict(adapter=self.adapter, run_id=self.run_id, phase=self.phase,
                    native_id=self.native_id, metadata=copy.deepcopy(self.metadata))

class ExecutionAdapter(ABC):
    @abstractmethod
    def capabilities(self): ...
    @abstractmethod
    def validate(self, spec): ...
    @abstractmethod
    def validate_target(self): ...
    @abstractmethod
    def dataset_key(self, spec, item): ...
    @abstractmethod
    def doctor(self): ...
    @abstractmethod
    def install(self): ...
    @abstractmethod
    def dataset_available(self, manifest): ...
    @abstractmethod
    def dataset_root(self, key): ...
    @abstractmethod
    def stage(self, job: PhaseJob): ...
    @abstractmethod
    def submit(self, staged) -> JobReference: ...
    @abstractmethod
    def status(self, reference: JobReference): ...
    @abstractmethod
    def logs(self, reference: JobReference, cursor=0): ...
    @abstractmethod
    def cancel(self, reference: JobReference): ...
    @abstractmethod
    def wait(self, reference: JobReference, timeout: int,
             cancelled: Callable, emit: Callable, log_path: Path): ...
