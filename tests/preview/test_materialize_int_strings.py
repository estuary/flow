import subprocess


def test_no_crash(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "preview-next",
            "--source",
            request.config.rootdir + "/tests/preview/test_materialize_int_strings.flow.yaml",
            "--fixture",
            request.config.rootdir + "/tests/preview/ints-strings-fixture.ndjson",
            "--sessions",
            "1,-1",  # Restart after the first transaction.
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
