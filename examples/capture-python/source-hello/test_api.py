# Validate runs the project's tests with pytest, from its `dev` dependency
# group, and fails if any don't pass. A test module is a listed file, like any
# other, and imports its task's modules relatively.
import asyncio
import logging

from estuary_cdk.capture.common import LogCursor
from examples.source_hello import EndpointConfig

from .api import fetch_greetings
from .models import Greeting


def fetch(log_cursor: LogCursor) -> list[Greeting | LogCursor]:
    async def collect() -> list[Greeting | LogCursor]:
        config = EndpointConfig(greeting="Hi", count=3)
        return [g async for g in fetch_greetings(config, logging.getLogger(), log_cursor)]

    return asyncio.run(collect())


def test_greetings_follow_the_cursor():
    assert fetch(1) == [
        Greeting(id=2, message="Hi #2!"),
        Greeting(id=3, message="Hi #3!"),
        3,
    ]


def test_no_greetings_follow_the_last():
    assert fetch(3) == []
