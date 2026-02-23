from __future__ import annotations

from dataclasses import dataclass
from typing import List
import base64


@dataclass
class DefinitionPart:
    path: str
    payload_b64: str

    def decode_utf8(self) -> str:
        return base64.b64decode(self.payload_b64).decode("utf-8", errors="replace")

    @staticmethod
    def from_text(path: str, text: str) -> "DefinitionPart":
        return DefinitionPart(path=path, payload_b64=base64.b64encode(text.encode("utf-8")).decode("ascii"))


@dataclass
class ItemDefinition:
    format: str
    parts: List[DefinitionPart]

    @staticmethod
    def from_api(defn: dict) -> "ItemDefinition":
        parts = [DefinitionPart(path=p["path"], payload_b64=p["payload"]) for p in defn.get("parts", [])]
        return ItemDefinition(format=defn.get("format", ""), parts=parts)

    def to_api(self) -> dict:
        return {"format": self.format, "parts": [{"path": p.path, "payload": p.payload_b64} for p in self.parts]}
