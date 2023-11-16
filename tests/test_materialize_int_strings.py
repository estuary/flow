import subprocess


def test_no_crash(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.config.rootdir + "/tests/test_materialize_int_strings.flow.yaml",
            "--fixture",
            request.config.rootdir + "/tests/ints-strings-fixture.json",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
