import asyncio

import source_hello

asyncio.run(source_hello.Connector().serve())
