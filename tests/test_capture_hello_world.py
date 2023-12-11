import json
import subprocess


def test_basic(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.config.rootdir + "/tests/test_capture_hello_world.flow.yaml",
            "--sessions",
            "3,2",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    for l in lines:
        l[1]["ts"] = "redacted-timestamp"

    assert snapshot("stdout.json") == lines
