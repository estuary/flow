from collections.abc import AsyncGenerator
from logging import Logger

from estuary_cdk.capture.common import LogCursor
from examples.source_hello import EndpointConfig

from .models import Greeting


async def fetch_greetings(
    config: EndpointConfig,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Greeting | LogCursor, None]:
    """Fetch greetings which follow `log_cursor`, and then checkpoint them.

    A real connector would request changes from its endpoint here,
    using the HTTP client passed to `all_resources`."""
    assert isinstance(log_cursor, int)

    if log_cursor >= config.count:
        return  # No new greetings are available.

    for id in range(log_cursor + 1, config.count + 1):
        yield Greeting(id=id, message=f"{config.greeting} #{id}!")

    yield config.count
