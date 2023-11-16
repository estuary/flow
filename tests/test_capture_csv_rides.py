import json
import subprocess


def test_basic(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.config.rootdir + "/tests/test_capture_csv_rides.flow.yaml",
            "--sessions",
            "3,-1",  # Restart after the tenth transaction.
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    # Remove a chunk of middle lines. Snapshot the beginning and end.
    lines = (
        lines[:100]
        + [f"... trimmed from {len(lines)} total lines... "]
        + lines[len(lines) - 100 :]
    )

    assert snapshot("stdout.json") == lines
