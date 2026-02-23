from __future__ import annotations

from dataclasses import dataclass, field
import yaml


@dataclass
class ColumnContract:
    name: str
    type: str
    nullable: bool = True
    description: str | None = None
    tags: list[str] = field(default_factory=list)


@dataclass
class DataContract:
    name: str
    version: int
    owner: str | None = None
    columns: list[ColumnContract] = field(default_factory=list)

    @staticmethod
    def load(path: str) -> "DataContract":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        cols = [ColumnContract(**c) for c in data.get("columns", [])]
        return DataContract(
            name=data.get("name", "unknown"),
            version=int(data.get("version", 1)),
            owner=data.get("owner"),
            columns=cols,
        )

    def dump(self, path: str) -> None:
        data = {
            "name": self.name,
            "version": self.version,
            "owner": self.owner,
            "columns": [c.__dict__ for c in self.columns],
        }
        with open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
