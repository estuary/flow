from .labels import LabelSet, percent_encoding
import pytest


# Tests
@pytest.fixture
def label_set():
    return LabelSet.build_set(
        [
            ("a", "1"),
            ("b", "2"),
            ("b", "3"),
            ("d", "4"),
            ("e", "5"),
            ("e", "6"),
            ("e:prefix", "7"),
        ]
    )


@pytest.mark.parametrize(
    "fixture, expect",
    [
        ("_", (0, 0)),
        ("a", (0, 1)),
        ("aa", (1, 1)),
        ("b", (1, 3)),
        ("c", (3, 3)),
        ("d", (3, 4)),
        ("dd", (4, 4)),
        ("e", (4, 7)),
        ("ee", (7, 7)),
        ("z", (7, 7)),
    ],
)
def test_label_range_cases(label_set, fixture, expect):
    assert label_set.range(fixture) == expect


def test_mutation_cases(snapshot):
    set = LabelSet.build_set([("a", "aa"), ("c", "cc"), ("d", "dd"), ("z", "")])

    set.add_value("a", "aa.2")
    set.set_value("d:prefix", "dd.2")
    set.add_value("b:prefix", "bb.1")
    set.remove("c")
    set.remove("z")

    assert snapshot("snap1.json") == set.model_dump(exclude_defaults=True)

    for v in ["aa.2", "aa.1", "aa.3", "aa.1", "aa.2", "aa.4", "aa.0"]:
        set.add_value("a", v)

    assert snapshot("snap2.json") == set.model_dump(exclude_defaults=True)


@pytest.mark.parametrize(
    "fixture, expect",
    [
        ("foo", "foo"),
        ("one/two", "one%2Ftwo"),
        ("hello, world!", "hello%2C%20world%21"),
        ("no.no&no-no@no$yes_yes();", "no.no%26no-no%40no%24yes_yes%28%29%3B"),
        (
            "http://example/path?q1=v1&q2=v2;ex%20tra",
            "http%3A%2F%2Fexample%2Fpath%3Fq1%3Dv1%26q2%3Dv2%3Bex%2520tra",
        ),
    ],
)
def test_percent_encode(fixture, expect):
    assert percent_encoding(fixture) == expect
