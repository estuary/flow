"""Pure pieces of the control-plane client: the 307 retry-delay arithmetic.

The wire-level behaviour (that 307s are absorbed at all) is covered by the
flow tests against the fake agent; these pin the header parsing, which the fake
always emits well-formed and production may not.
"""

import time

import httpx

from estuary_mcp import control_plane


def _response(headers: dict[str, str]) -> httpx.Response:
    return httpx.Response(status_code=307, headers=headers)


def _no_deadline() -> float:
    return time.monotonic() + 1000


def test_retry_delay_is_computed_against_the_servers_own_clock():
    """`Retry-After` is an absolute date, so it is compared to the agent's `Date`
    header rather than our clock — the two machines need not agree."""
    delay = control_plane._retry_delay(
        _response(
            {
                "date": "Mon, 17 Aug 2026 12:00:00 GMT",
                "retry-after": "Mon, 17 Aug 2026 12:00:03 GMT",
            }
        ),
        deadline=_no_deadline(),
    )
    assert delay == 3.0


def test_missing_retry_after_means_replay_immediately():
    """No Retry-After means the agent chose to block server-side instead."""
    assert control_plane._retry_delay(_response({}), deadline=_no_deadline()) == 0.0


def test_unparseable_retry_after_degrades_to_an_immediate_replay():
    delay = control_plane._retry_delay(
        _response({"date": "Mon, 17 Aug 2026 12:00:00 GMT", "retry-after": "soonish"}),
        deadline=_no_deadline(),
    )
    assert delay == 0.0


def test_retry_delay_is_clamped_to_the_overall_deadline():
    delay = control_plane._retry_delay(
        _response(
            {
                "date": "Mon, 17 Aug 2026 12:00:00 GMT",
                "retry-after": "Mon, 17 Aug 2026 12:05:00 GMT",
            }
        ),
        deadline=time.monotonic() + 2.0,
    )
    assert 0.0 <= delay <= 2.0
