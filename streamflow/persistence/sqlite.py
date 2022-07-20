from __future__ import annotations

import os
import sqlite3
from typing import Any, MutableMapping, Optional, TYPE_CHECKING, Type, TypeVar

import pandas as pd

from streamflow.core.persistence import Database, DependencyType
from streamflow.core.workflow import Step

if TYPE_CHECKING:
    S = TypeVar('S', bound=Step)


class SqliteDatabase(Database):

    def __init__(self,
                 connection: str,
                 reset_db: bool = False):
        # If needed, reset the database
        if reset_db and os.path.isfile(connection):
            os.remove(connection)
        # Open connection to database
        os.makedirs(os.path.dirname(connection), exist_ok=True)
        if reset_db or not os.path.exists(connection):
            self.connection = sqlite3.Connection(connection)
            self._init_db()
        else:
            self.connection = sqlite3.Connection(connection)
        cursor = self.connection.cursor()
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("PRAGMA wal_autocheckpoint = 10")

    def __del__(self):
        # Force connection close
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()

    def _init_db(self):
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schemas', 'sqlite.sql')
        with open(schema_path, "r") as f:
            with self.connection as db:
                db.cursor().executescript(f.read())

    def add_workflow(self,
                     name: str,
                     status: int,
                     wf_type: str) -> int:
        with self.connection as db:
            cursor = db.cursor()
            cursor.execute("INSERT INTO workflow(name, status, type) VALUES(:name, :status, :type)", {
                "name": name,
                "status": status,
                "type": wf_type})
            return cursor.lastrowid

    def update_workflow(self,
                        workflow_id: int,
                        updates: MutableMapping[str, Any]) -> int:
        with self.connection as db:
            db.cursor().execute("UPDATE workflow SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": workflow_id}})
            return workflow_id

    def get_workflows(self) -> pd.DataFrame:
        return pd.read_sql_query("SELECT name, status, type FROM workflow ORDER BY id DESC", self.connection)

    def add_step(self,
                 name: str,
                 workflow_id: int,
                 status: int,
                 step_type: Type[S],
                 params: str) -> int:
        with self.connection as db:
            cursor = db.cursor()
            cursor.execute(
                "INSERT INTO step(name, workflow, status, type, params) "
                "VALUES(:name, :workflow, :status, :type, :params)", {
                    "name": name,
                    "workflow": workflow_id,
                    "status": status,
                    "type": step_type.__module__ + '.' + step_type.__qualname__,
                    "params": params})
            return cursor.lastrowid

    def update_step(self,
                    step_id: int,
                    updates: MutableMapping[str, Any]) -> int:
        with self.connection as db:
            db.cursor().execute("UPDATE step SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": step_id}})
            return step_id

    def get_steps(self,
                  workflow_id: int) -> pd.DataFrame:
        return pd.read_sql_query(
            sql="SELECT * FROM step WHERE workflow = :workflow",
            con=self.connection,
            params={"workflow": workflow_id})

    def add_port(self,
                 name: str,
                 workflow_id: int,
                 port_type: Type[S],
                 params: str) -> int:
        with self.connection as db:
            cursor = db.cursor()
            cursor.execute(
                "INSERT INTO port(name, workflow, type, params) "
                "VALUES(:name, :workflow, :type, :params)", {
                    "name": name,
                    "workflow": workflow_id,
                    "type": port_type.__module__ + '.' + port_type.__qualname__,
                    "params": params})
            return cursor.lastrowid

    def update_port(self,
                    port_id: int,
                    updates: MutableMapping[str, Any]) -> int:
        with self.connection as db:
            db.cursor().execute("UPDATE port SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": port_id}})
            return port_id

    def get_ports(self,
                  workflow_id: int) -> pd.DataFrame:
        return pd.read_sql_query(
            sql="SELECT * FROM port WHERE workflow = :workflow",
            con=self.connection,
            params={"workflow": workflow_id})

    def add_dependency(self,
                       step: int,
                       port: int,
                       dep_type: DependencyType,
                       name: str) -> None:
        with self.connection as db:
            cursor = db.cursor()
            cursor.execute(
                "INSERT INTO dependency(step, port, type, name) "
                "VALUES(:step, :port, :type, :name)", {
                    "step": step,
                    "port": port,
                    "type": dep_type.value,
                    "name": name})

    def add_deployment(self,
                       name: str,
                       connector_type: str,
                       external: bool,
                       params: str) -> int:
        with self.connection as db:
            cursor = db.cursor()
            cursor.execute(
                "INSERT INTO deployment(name, type, params, external) "
                "VALUES (:name, :type, :params, :external)", {
                    "name": name,
                    "type": connector_type,
                    "params": params,
                    "external": external})
            return cursor.lastrowid

    def update_deployment(self,
                          deployment_id: int,
                          updates: MutableMapping[str, Any]) -> int:
        with self.connection as db:
            db.cursor().execute("UPDATE deployment SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": deployment_id}})
            return deployment_id

    def add_target(self,
                   deployment: int,
                   locations: int = 1,
                   service: Optional[str] = None,
                   workdir: Optional[str] = None) -> int:
        with self.connection as db:
            cursor = db.cursor()
            cursor.execute(
                "INSERT INTO target(deployment, locations, service, workdir) "
                "VALUES (:deployment, :locations, :service, :workdir)", {
                    "deployment": deployment,
                    "locations": locations,
                    "service": service,
                    "workdir": workdir})
            return cursor.lastrowid

    def update_target(self,
                      target_id: int,
                      updates: MutableMapping[str, Any]) -> int:
        with self.connection as db:
            db.cursor().execute("UPDATE target SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": target_id}})
            return target_id

    def add_command(self,
                    step_id: int,
                    cmd: str) -> int:
        with self.connection as db:
            cursor = db.cursor()
            cursor.execute("INSERT INTO command(step, cmd) VALUES(:step, :cmd)", {
                "step": step_id,
                "cmd": cmd})
            return cursor.lastrowid

    def update_command(self,
                       command_id: int,
                       updates: MutableMapping[str, Any]) -> int:
        with self.connection as db:
            db.cursor().execute("UPDATE command SET {} WHERE id = :id".format(
                ", ".join(["{} = :{}".format(k, k) for k in updates])
            ), {**updates, **{"id": command_id}})
            return command_id

    def get_report(self,
                   workflow: str) -> pd.DataFrame:
        return pd.read_sql_query(
            sql="SELECT c.id, s.name, c.start_time, c.end_time FROM step AS s, command AS c "
                "WHERE s.id = c.step AND s.workflow = (SELECT id FROM workflow WHERE name = :workflow)",
            con=self.connection,
            params={"workflow": workflow})
