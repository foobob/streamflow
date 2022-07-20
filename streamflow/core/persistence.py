from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, MutableMapping, Optional, TYPE_CHECKING, Type, TypeVar

import pandas as pd

if TYPE_CHECKING:
    from streamflow.core.workflow import Step

    S = TypeVar('S', bound=Step)


class PersistableEntity(object):

    def __init__(self):
        self.persistent_id: Optional[int] = None

    @abstractmethod
    def save(self) -> str:
        ...


class DependencyType(Enum):
    INPUT = 0
    OUTPUT = 1


class Database(ABC):

    @abstractmethod
    def add_workflow(self,
                     name: str,
                     status: int,
                     wf_type: str) -> int:
        ...

    @abstractmethod
    def update_workflow(self,
                        workflow_id: int,
                        updates: MutableMapping[str, Any]) -> int:
        ...

    @abstractmethod
    def get_workflows(self) -> pd.DataFrame:
        ...

    @abstractmethod
    def add_step(self,
                 name: str,
                 workflow_id: int,
                 status: int,
                 step_type: Type[S],
                 params: str) -> int:
        ...

    @abstractmethod
    def update_step(self,
                    step_id: int,
                    updates: MutableMapping[str, Any]) -> int:
        ...

    @abstractmethod
    def get_steps(self,
                  workflow_id: int) -> pd.DataFrame:
        ...

    @abstractmethod
    def add_port(self,
                 name: str,
                 workflow_id: int,
                 port_type: Type[S],
                 params: str) -> int:
        ...

    @abstractmethod
    def update_port(self,
                    port_id: int,
                    updates: MutableMapping[str, Any]) -> int:
        ...

    @abstractmethod
    def get_ports(self,
                  workflow_id: int) -> pd.DataFrame:
        ...

    @abstractmethod
    def add_dependency(self,
                       step: int,
                       port: int,
                       dep_type: DependencyType, name: str) -> None:
        ...

    @abstractmethod
    def add_deployment(self,
                       name: str,
                       connector_type: str,
                       external: bool,
                       params: str) -> int:
        ...

    @abstractmethod
    def update_deployment(self,
                          deployment_id: int,
                          updates: MutableMapping[str, Any]) -> int:
        ...

    @abstractmethod
    def add_target(self,
                   deployment: int,
                   locations: int = 1,
                   service: Optional[str] = None,
                   workdir: Optional[str] = None) -> int:
        ...

    @abstractmethod
    def update_target(self,
                      target_id: str,
                      updates: MutableMapping[str, Any]) -> int:
        ...

    @abstractmethod
    def add_command(self,
                    step_id: int,
                    cmd: str) -> int:
        ...

    @abstractmethod
    def update_command(self,
                       command_id: int,
                       updates: MutableMapping[str, Any]) -> int:
        ...

    @abstractmethod
    def get_report(self,
                   workflow: str) -> pd.DataFrame:
        ...
