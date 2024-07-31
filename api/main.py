from fastapi import FastAPI, Request, HTTPException, responses
from pydantic import BaseModel, Field, ValidationError
import logging
import jwt
import asyncpg
import base64
import time

from .models.labels import NAME, percent_encoding
from .models.claims import Claims, Capability

from .logger import init_logger

logger = init_logger()

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

# engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres")

app = FastAPI()

ALL_KEYS = {
    "localhost": {
        "keys": [base64.b64decode(s) for s in ("c2VjcmV0", "b3RoZXItc2VjcmV0")],
        "logs": "ops.us-central1.v1/logs",
        "stats": "ops.us-central1.v1/stats",
    }
}


@app.exception_handler(jwt.DecodeError)
async def jwt_decode_handler(request: Request, exc: jwt.DecodeError):
    return responses.JSONResponse(
        status_code=401,
        content={"message": f"Failed to decode Authentication token: {exc}"},
    )


@app.exception_handler(jwt.ExpiredSignatureError)
async def jwt_expired_handler(request: Request, exc: jwt.ExpiredSignatureError):
    return responses.JSONResponse(
        status_code=401,
        content={"message": f"Authentication token has expired: {exc}"},
    )


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/authorize")
async def authorize(request: Request) -> responses.PlainTextResponse:
    token = (await request.body()).decode("utf8")

    # Parse the token's claims without verifying them.
    try:
        claims = Claims(**jwt.decode(token, options={"verify_signature": False}))
    except ValidationError as ex:
        raise HTTPException(status_code=422, detail=ex.errors())

    # Identify suitable keys for verifying the issuing data plane.
    if not (data_plane := ALL_KEYS.get(claims.issuer)):
        raise HTTPException(
            status_code=400, detail=f"issuer '{claims.issuer}' is unknown"
        )

    matched = False
    for key in data_plane["keys"]:
        try:
            _ = jwt.decode(token, key, algorithms=["HS256", "HS384"])
            matched = True
        except jwt.DecodeError as ex:
            pass

    if not matched:
        raise HTTPException(
            status_code=401, detail="no key matched the issuing data-plane signature"
        )

    # Split off the leading 'capture', 'derivation', or 'materialization'
    # prefix of the Shard ID conveyed in `claims.subject`
    task_type, task_shard = claims.subject.split("/", 1)

    journal_name_or_prefix = claims.selector.include.expect_one("name")

    # Validate and match the requested capabilities to a corresponding role.
    required_role: str
    if claims.capability in (
        Capability.AUTHORIZE | Capability.LIST,
        Capability.AUTHORIZE | Capability.READ,
    ):
        required_role = "read"
    elif claims.capability in (
        Capability.AUTHORIZE | Capability.APPLY,
        Capability.AUTHORIZE | Capability.APPEND,
    ):
        required_role = "write"
    else:
        raise HTTPException(
            status_code=401,
            detail=f"Capability {claims.capability} cannot be authorized by this service",
        )

    conn = await asyncpg.connect(
        user="postgres", password="postgres", database="postgres", host="localhost"
    )

    row = await conn.fetchrow(
        """
        -- TODO(johnny): factor into role_roles() function ?
        with recursive
        role_roles(role_prefix, capability) as (
                select g.object_role, g.capability
                from role_grants g
                where starts_with($1, g.subject_role)
                  and g.capability >= $2
            union
                select g.object_role, g.capability
                from role_grants g, role_roles a
                where starts_with(a.role_prefix, g.subject_role)
                  and g.capability >= $2
                  and a.capability = 'admin'
        )
        -- TODO -- verify that task is in the correct declared data plane.
        -- TODO -- extract the data plane of the collection
        select
            t.catalog_name,
            c.catalog_name,
            exists(
                select 1 from role_roles r where starts_with($3, r.role_prefix)
            )
        from live_specs t, live_specs c
        where starts_with($1, t.catalog_name)
          and starts_with($3, c.catalog_name)
        """,
        task_shard,
        required_role,
        journal_name_or_prefix,
    )
    task_name, collection_name, authorized = row.values() if row else ("", "", False)

    await conn.close()

    logger.info(
        "AUTHORIZE",
        extra={
            "journal_name_or_prefix": journal_name_or_prefix,
            "spec_type": task_type,
            "shard_id": task_shard,
            "access_level": required_role,
            "suffix": f"/kind={task_type}/name={percent_encoding(task_name)}/pivot=00",
        },
    )

    # Allow a task to write to its designated partition of ops collections.
    if (
        not authorized
        and required_role == "write"
        and collection_name
        in (ALL_KEYS["localhost"]["logs"], ALL_KEYS["localhost"]["stats"])
        and journal_name_or_prefix.endswith(
            f"/kind={task_type}/name={percent_encoding(task_name)}/pivot=00"
        )
    ):
        authorized = True

    if not authorized:
        raise HTTPException(
            status_code=401,
            detail=f"Task shard {task_shard} is not authorized to {journal_name_or_prefix} for {required_role}",
        )

    # We've now verified RBAC
    print(task_name, collection_name)

    claims.issuer = "localhost"  # TODO target data plane
    claims.capability = Capability(claims.capability - Capability.AUTHORIZE)
    claims.issuedAt = int(time.time())
    claims.expiresAt = claims.issuedAt + 3600  # One hour.

    token = jwt.encode(
        claims.model_dump(by_alias=True, exclude_unset=True),
        ALL_KEYS["localhost"]["keys"][0],
        algorithm="HS256",
    )

    return responses.PlainTextResponse(token, status_code=200)
