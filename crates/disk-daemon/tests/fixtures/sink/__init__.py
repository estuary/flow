"""Counts its transactions in SQLite on its disk, and in its connector state.

Its disk commits with its connector state. So at open, the two counts are
equal, unless the disk lost or repeated a transaction.
"""

import os
import sqlite3
import typing
from collections.abc import AsyncIterator

from estuary_cdk.flow import ConnectorStateUpdate
from pydantic import BaseModel

from acmeCo.disks.sink import (
    EndpointConfig,
    IMaterialization,
    Request,
    ResourceConfig,
    Response,
)


class State(BaseModel):
    transactions: int = 0


class Materialization(IMaterialization[EndpointConfig, ResourceConfig, State]):
    def __init__(self, db: sqlite3.Connection, transactions: int):
        self.db = db
        self.transactions = transactions

    @classmethod
    async def open(
        cls, open: Request.Open[EndpointConfig, ResourceConfig, State]
    ) -> tuple[typing.Self, Response.Opened]:
        db = sqlite3.connect(
            os.path.join(os.environ["CONNECTOR_MOUNT"], "persist", "transactions.db")
        )
        db.execute("PRAGMA synchronous = OFF")
        # `user_version` is an integer in the database header, kept for applications.
        on_disk: int = db.execute("PRAGMA user_version").fetchone()[0]

        if on_disk != open.state.transactions:
            raise RuntimeError(
                f"the disk counts {on_disk} transactions, and the connector state counts {open.state.transactions}: the disk lost or repeated a transaction"
            )
        return cls(db, on_disk), Response.Opened()

    async def load(self, load: Request.Load[typing.Any]) -> AsyncIterator[Response.Loaded[typing.Any]]:
        if False:
            yield

    async def store(self, store: Request.Store[typing.Any, typing.Any]) -> None:
        pass

    async def start_commit(
        self, start_commit: Request.StartCommit, started: Response.StartedCommit[State]
    ) -> None:
        self.transactions += 1
        self.db.execute(f"PRAGMA user_version = {self.transactions}")
        started.state = ConnectorStateUpdate[State](
            updated=State(transactions=self.transactions), mergePatch=True
        )
