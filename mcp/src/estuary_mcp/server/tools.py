"""The MCP tools this adapter exposes.

There is exactly one, on purpose. The point of this prototype is the
authorization mechanics; the tool is the acceptance test for them. `prefixes`
was chosen because it is the simplest call that *cannot* succeed without a valid
control-plane access token belonging to a real user — so one green tool call
proves the entire chain: CIMD identification, dashboard consent, credential
mint, refresh rotation, bearer pass-through, control-plane verification, and
grant-graph authorization.

Adding tools is the easy part and should wait until the authorization story is
settled. When it is, note that every tool follows the same two-line shape below:
take the caller's bearer from the request context, hand it to the control plane,
return what comes back. Nothing here should ever acquire a credential of its own.
"""

import logging
from typing import Any

from mcp.server.auth.middleware.auth_context import get_access_token

from .. import control_plane

log = logging.getLogger(__name__)

# `by: {minCapability: read}` is the widest view: every prefix the user can see
# at all. `capabilities` is the fine-grained bit set; `userCapability` is the
# legacy coarse column, included because it is what most Estuary docs speak in.
PREFIXES_QUERY = """
query EstuaryMcpPrefixes($first: Int!) {
  prefixes(by: { minCapability: read }, first: $first) {
    edges {
      node {
        prefix
        userCapability
        capabilities
      }
    }
    pageInfo {
      hasNextPage
    }
  }
}
"""

# One page is plenty to prove the chain works, and a tool that quietly paginated
# forever would be a poor citizen of a model's context window. `has_more` in the
# result tells the caller when they are seeing a truncated view.
PREFIX_PAGE_SIZE = 200


class ToolError(Exception):
    """A tool could not complete. The message reaches the model, so it is phrased
    as something a model or user can act on."""


async def prefixes(agent: control_plane.ControlPlane) -> dict[str, Any]:
    """List the catalog prefixes the authenticated user can access.

    Prefixes are Estuary's unit of authorization: a `/`-delimited path like
    `acmeCo/` names both a namespace and the role that governs it, and a user's
    capabilities are grants against those roles.
    """
    bearer = _caller_bearer()

    try:
        data = await agent.graphql(
            bearer, PREFIXES_QUERY, variables={"first": PREFIX_PAGE_SIZE}
        )
    except control_plane.ControlPlaneError as err:
        if err.is_unauthorized:
            # The transport-level 401 that would normally prompt a client to
            # refresh has already been passed: we are inside a JSON-RPC result.
            # Say plainly what happened so the client's next `/mcp` request —
            # which will 401 on the expired token — is understood as a refresh
            # cue rather than a broken tool.
            raise ToolError(
                "The Estuary control plane rejected this credential. "
                "Re-authenticate with the Estuary MCP server and try again."
            ) from err
        raise ToolError(f"The Estuary control plane could not be reached: {err}") from err

    connection = data.get("prefixes") or {}
    nodes = [edge["node"] for edge in connection.get("edges", []) if "node" in edge]

    return {
        "prefixes": nodes,
        "has_more": bool((connection.get("pageInfo") or {}).get("hasNextPage")),
    }


def _caller_bearer() -> str:
    """The access token the MCP client presented on this request.

    The SDK stows it in a contextvar during request handling. Its absence means
    the request reached a tool without passing `RequireAuthMiddleware`, which
    would be a wiring bug rather than an authorization failure — so this is an
    assertion, not error handling.
    """
    access_token = get_access_token()
    assert access_token is not None, "tool invoked outside an authenticated request"
    return access_token.token
