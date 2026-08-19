"""Process entry point: read the environment, build the app, serve it."""

import logging
import sys

import uvicorn

from . import app as app_module
from . import config


def main() -> int:
    try:
        settings = config.from_env()
    except config.ConfigError as err:
        # Configuration is checked before anything binds a socket: an adapter
        # with the wrong `public_url` would happily serve OAuth metadata that
        # sends browsers somewhere else entirely.
        print(f"estuary-mcp: {err}", file=sys.stderr)
        return 2

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    log = logging.getLogger("estuary_mcp")
    log.info("public url : %s", settings.public_url)
    log.info("agent      : %s", settings.agent_url)
    log.info("dashboard  : %s", settings.dashboard_url)
    log.info("listening  : %s:%d", settings.bind_host, settings.bind_port)

    uvicorn.run(
        app_module.build_app(settings),
        host=settings.bind_host,
        port=settings.bind_port,
        log_level=settings.log_level.lower(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
