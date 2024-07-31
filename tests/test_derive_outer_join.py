import json
import subprocess


def test_with_restart(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.config.rootdir + "/examples/derive-patterns/join-outer.flow.yaml",
            "--name",
            "patterns/outer-join",
            "--fixture",
            request.config.rootdir + "/tests/ints-strings-fixture.json",
            "--sessions",
            "2,-1",  # Restart after the second transaction.
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("stdout.json") == lines
