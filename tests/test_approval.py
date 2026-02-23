import json
import pytest

from fabric_agent.core.approval import approve_plan_file, require_approved


def test_require_approved_blocks_pending(tmp_path):
    p = tmp_path / "plan.json"
    p.write_text(json.dumps({"approval": {"status": "PENDING"}}), encoding="utf-8")
    plan = json.loads(p.read_text(encoding="utf-8"))
    with pytest.raises(RuntimeError):
        require_approved(plan)


def test_approve_plan_file_stamps_approval(tmp_path):
    p = tmp_path / "plan.json"
    p.write_text(json.dumps({"operation_id": "op_test", "approval": {"status": "PENDING"}}), encoding="utf-8")
    updated = approve_plan_file(str(p), approver="Alice", comment="ok")
    assert updated["approval"]["status"] == "APPROVED"
    assert updated["approval"]["approved_by"] == "Alice"
    # now should not raise
    require_approved(updated)
