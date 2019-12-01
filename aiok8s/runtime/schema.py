from typing import NamedTuple


class GroupVersionKind(NamedTuple):
    group: str
    version: str
    kind: str

    def __str__(self):
        return f"{self.group}/{self.version}: kind={self.kind}"
