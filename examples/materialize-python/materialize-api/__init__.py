"""A reverse-ETL materialization into the example's API of keyed records.

- Binding `crm_contacts` reshapes each contact into the record which the API
  expects. It uses delta updates, and is never loaded.
- Binding `page_views` uses standard updates, whose views the runtime sums.
  Its loads buffer their keys, which are fetched in batches.
- Any other binding is written by the generic `store`, as its documents, to the
  table of its resource path.

Writes are buffered and sent as the transaction commits. The API isn't
transactional, so a transaction which fails to commit sends its writes again
when it's retried. They're upserts, which are idempotent.
"""

import typing
from collections.abc import AsyncIterator

from estuary_cdk.flow import MaterializationBinding
from examples.materialize_api import (
    EndpointConfig,
    IMaterialization,
    Request,
    ResourceConfig,
    Response,
    SourcePageViews,
)

from .client import Client


class Materialization(IMaterialization):
    # Buffered loads are fetched in batches of this size. It's small, so that
    # the example fetches some batches as they fill and others upon flush.
    LOAD_BATCH = 2

    def __init__(self, client: Client, bindings: list[MaterializationBinding[ResourceConfig]]):
        self.client = client
        self.bindings = bindings
        # Keys of `page_views` to load, and its binding index.
        self.load_keys: list[list[typing.Any]] = []
        self.load_binding = 0
        # Records to write, keyed on their table.
        self.rows: dict[str, list[dict[str, typing.Any]]] = {}

    @classmethod
    async def open(
        cls, open: Request.Open[EndpointConfig, ResourceConfig, dict[str, typing.Any]]
    ) -> tuple[typing.Self, Response.Opened]:
        client = Client(open.materialization.config.baseUrl)
        return cls(client, open.materialization.bindings), Response.Opened()

    @classmethod
    async def apply(
        cls,
        apply: Request.Apply[EndpointConfig, ResourceConfig, dict[str, typing.Any]],
        applied: Response.Applied[dict[str, typing.Any]],
    ) -> None:
        client = Client(apply.materialization.config.baseUrl)
        tables = [binding.resourcePath[-1] for binding in apply.materialization.bindings]
        for table in tables:
            await client.create(table)

        applied.actionDescription = f"created tables {', '.join(tables)}, if they didn't exist"

    async def store_crm_contacts(self, store: Request.StoreCrmContacts) -> None:
        doc = store.doc
        self.write(
            "crm_contacts",
            store.key,
            None if store.delete else {
                "email": doc.email,
                "firstname": doc.firstName,
                "lastname": doc.lastName,
            },
        )

    async def load_page_views(self, load: Request.LoadPageViews) -> AsyncIterator[Response.Loaded[typing.Any]]:
        self.load_binding = load.binding
        self.load_keys.append(list(load.key))

        if len(self.load_keys) < self.LOAD_BATCH:
            return
        # Swap out the full batch before awaiting, so that concurrent loads
        # buffer their keys into the next batch.
        keys, self.load_keys = self.load_keys, []
        async for loaded in self.fetch(keys):
            yield loaded

    async def flush(
        self, flush: Request.Flush, flushed: Response.Flushed[dict[str, typing.Any]]
    ) -> AsyncIterator[Response.Loaded[typing.Any]]:
        keys, self.load_keys = self.load_keys, []
        if keys:
            async for loaded in self.fetch(keys):
                yield loaded

    async def fetch(self, keys: list[list[typing.Any]]) -> AsyncIterator[Response.Loaded[typing.Any]]:
        for row in await self.client.get("page_views", keys):
            yield Response.Loaded(binding=self.load_binding, doc=SourcePageViews.model_validate(row["record"]))

    async def store_page_views(self, store: Request.StorePageViews) -> None:
        self.write(
            "page_views",
            store.key,
            None if store.delete else store.doc.model_dump(by_alias=True, exclude_unset=True),
        )

    async def store(self, store: Request.Store[typing.Any, typing.Any]) -> None:
        table = self.bindings[store.binding].resourcePath[-1]
        doc = store.doc.model_dump(mode="json", by_alias=True, exclude_unset=True)
        self.write(table, store.key, None if store.delete else doc)

    async def load(self, load: Request.Load[typing.Any]) -> AsyncIterator[Response.Loaded[typing.Any]]:
        if False:
            yield
        raise RuntimeError(f"unhandled load of binding {self.bindings[load.binding].resourcePath}")

    def write(self, table: str, key: typing.Sequence[typing.Any], record: typing.Any) -> None:
        self.rows.setdefault(table, []).append({"key": list(key), "record": record})

    async def start_commit(
        self, start_commit: Request.StartCommit, started: Response.StartedCommit[dict[str, typing.Any]]
    ) -> None:
        rows, self.rows = self.rows, {}
        for table, table_rows in rows.items():
            await self.client.put(table, table_rows)
