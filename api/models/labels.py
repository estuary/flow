from pydantic import BaseModel, Field
from bisect import bisect_left, bisect_right
from typing import Iterable
import functools
import urllib.parse

NAME = "name"
COLLECTION = "estuary.dev/collection"


@functools.total_ordering
class Label(BaseModel, extra="forbid"):
    name: str
    value: str = ""
    prefix: bool = False

    def __lt__(self, other):
        if not isinstance(other, Label):
            return NotImplemented
        if self.name == other.name:
            return self.value < other.value
        return self.name < other.name

    def __eq__(self, other):
        if not isinstance(other, Label):
            return NotImplemented
        return self.name == other.name and self.value == other.value


class LabelSet(BaseModel, extra="forbid"):
    labels: list[Label]

    @staticmethod
    def build_set(it: Iterable[tuple[str, str]]) -> "LabelSet":
        set = LabelSet(labels=[])
        for name, value in it:
            set.add_value(name, value)
        return set

    def range(self, name: str) -> tuple[int, int]:
        lo = bisect_left(self.labels, name, key=lambda label: label.name)
        hi = bisect_right(self.labels, name, lo=lo, key=lambda label: label.name)
        return lo, hi

    def values(self, name: str) -> list[Label]:
        lo, hi = self.range(name)
        return self.labels[lo:hi]

    def set_value(self, name: str, value: str):
        name, prefix = (name[:-7], True) if name.endswith(":prefix") else (name, False)
        lo, hi = self.range(name)
        self.labels[lo:hi] = (Label(name=name, value=value, prefix=prefix),)
        return self

    def add_value(self, name: str, value: str):
        name, prefix = (name[:-7], True) if name.endswith(":prefix") else (name, False)

        target = Label(name=name, value=value, prefix=prefix)
        idx = bisect_left(self.labels, target)

        if idx != len(self.labels) and self.labels[idx] == target:
            return self  # `value` is already present.

        self.labels.insert(idx, target)

    def remove(self, name: str):
        lo, hi = self.range(name)
        del self.labels[lo:hi]

    def expect_one_u32(self, name: str) -> int:
        value = self.expect_one(name)
        if len(value) != 8 or not all(c in "0123456789abcdefABCDEF" for c in value):
            raise InvalidValueError(name, value)
        return int(value, 16)

    def expect_one(self, name: str) -> str:
        labels = self.values(name)
        if len(labels) != 1:
            raise ExpectedOneError(name, labels)
        if labels[0].value == "":
            raise ValueEmptyError(name)
        return labels[0].value

    def maybe_one(self, name: str) -> str:
        labels = self.values(name)
        if len(labels) > 1:
            raise ExpectedOneError(name, labels)
        if not labels:
            return ""
        if labels[0].value == "":
            raise ValueEmptyError(name)
        return labels[0].value


class LabelSelector(BaseModel, extra="forbid"):
    include: LabelSet = Field(default_factory=lambda: LabelSet(labels=[]))
    exclude: LabelSet = Field(default_factory=lambda: LabelSet(labels=[]))


# Custom Exceptions
class LabelError(Exception):
    pass


class ExpectedOneError(LabelError):
    def __init__(self, name: str, labels: list[Label]):
        self.name = name
        self.labels = labels
        super().__init__(
            f"Expected one label for '{name}', but found {len(labels)}: {labels}"
        )


class ValueEmptyError(LabelError):
    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Value for label '{name}' is empty")


class InvalidValueError(LabelError):
    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value
        super().__init__(f"Invalid value for label '{name}': {value}")


def percent_encoding(s: str) -> str:
    safe_chars = "-_."
    return urllib.parse.quote(s, safe=safe_chars)
