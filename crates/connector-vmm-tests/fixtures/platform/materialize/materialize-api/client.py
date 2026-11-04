"""Client of the example's API of keyed records (`api.py`)."""

import typing

import aiohttp


class Client:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    async def create(self, table: str) -> None:
        await self.request("PUT", f"/tables/{table}", None)

    async def get(self, table: str, keys: list[list[typing.Any]]) -> list[dict[str, typing.Any]]:
        """Stored records of `keys` which exist, each as {"key", "record"}."""
        return await self.request("POST", f"/tables/{table}/get", {"keys": keys})

    async def put(self, table: str, rows: list[dict[str, typing.Any]]) -> None:
        """Upsert each {"key", "record"} of `rows`, or delete it if its record is None."""
        await self.request("POST", f"/tables/{table}/put", {"rows": rows})

    async def request(self, method: str, path: str, body: typing.Any) -> typing.Any:
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.request(method, self.base_url + path, json=body) as response:
                return await response.json()
