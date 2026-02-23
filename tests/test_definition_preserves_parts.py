import base64
from fabric_agent.refactor.models import ItemDefinition
from fabric_agent.refactor.safe_rename_engine import _rewrite_definition

def b64(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("ascii")

def test_rewrite_preserves_untouched_parts_byte_for_byte():
    parts = [
        {"path": "a.json", "payload": b64('{"x": "[Sales]"}')},
        {"path": "b.json", "payload": b64('{"y": "nope"}')},
    ]
    before = ItemDefinition.from_api({"format": "PBIR", "parts": parts})
    after, changed, _hits = _rewrite_definition(before, "[Sales]", "[Revenue]")
    assert changed == ["a.json"]
    before_b = [p for p in before.parts if p.path == "b.json"][0].payload_b64
    after_b = [p for p in after.parts if p.path == "b.json"][0].payload_b64
    assert before_b == after_b
