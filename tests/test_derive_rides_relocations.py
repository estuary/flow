import json
import subprocess


def test_with_restart(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.config.rootdir
            + "/examples/citi-bike/rides-and-relocations.flow.yaml",
            "--name",
            "examples/citi-bike/rides-and-relocations",
            "--fixture",
            request.config.rootdir + "/tests/rides-fixture.json",
            "--sessions",
            "1,-1",  # Restart after the first transaction.
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("stdout.json") == lines
