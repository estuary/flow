from pydantic import BaseModel, Field
from enum import IntFlag
from .labels import LabelSelector


class Claims(BaseModel):
    # Claims defined by the standard.
    issuer: str = Field(alias="iss")
    subject: str = Field(alias="sub")
    issuedAt: int = Field(alias="iat")
    expiresAt: int = Field(alias="exp")

    # Claims defined by us.
    capability: "Capability" = Field(alias="cap")
    selector: LabelSelector = Field(alias="sel")


class Capability(IntFlag):

    LIST = 1 << 1
    """
    LIST gives the bearer a capability to use Gazette's List API.
    """

    APPLY = 1 << 2
    """
    APPLY gives the bearer a capability to use Gazette's Apply API.
    """

    READ = 1 << 3
    """
    APPLY gives the bearer a capability to use Gazette's Read API.
    """

    APPEND = 1 << 4
    """
    APPEND gives the bearer a capability to use Gazette's Append API.
    """

    REPLICATE = 1 << 5
    """
    REPLICATE gives the bearer a capability to use Gazette's internal Replicate API.
    """

    AUTHORIZE = 1 << 16
    """
    AUTHORIZE gives the bearer a capability to request an authorization
    for the given claim, which may then be signed using a different key
    and returned without the AUTHORIZE capability (which prevents the
    recipient from using the token to obtain further Authorizations).
    """

    SHUFFLE = 1 << 17
    """
	SHUFFLE gives the bearer a capability to use the runtime's Shuffle API.
    """

    NETWORK_PROXY = 1 << 18
    """
	NETWORK_PROXY gives the bearer a capability to use the runtime's Network Proxy API.
    """
