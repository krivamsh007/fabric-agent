import base64

from fabric_agent.reporting import render_safe_refactor_html, render_schema_drift_html


def b64(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def test_render_safe_refactor_html_creates_file(tmp_path):
    plan_doc = {
        "operation_id": "op_test",
        "ts": 0,
        "workspace_id": "ws",
        "semantic_model_id": "sm",
        "old": "[Sales]",
        "new": "[Revenue]",
        "impact": {
            "semantic_model_parts_changed": ["model.tmdl"],
            "reports_changed": {"rep1": ["report.json"]},
            "risk": "LOW",
            "notes": [],
        },
        "approval": {"status": "PENDING", "approved_by": None, "comment": None, "approved_ts": None},
    }

    sm_before = {"format": "TMDL", "parts": [{"path": "model.tmdl", "payload": b64("MEASURE=[Sales]")}]}
    sm_after = {"format": "TMDL", "parts": [{"path": "model.tmdl", "payload": b64("MEASURE=[Revenue]")}]}

    rep_before = {"format": "PBIR", "parts": [{"path": "report.json", "payload": b64('{"m":"[Sales]"}')}]}
    rep_after = {"format": "PBIR", "parts": [{"path": "report.json", "payload": b64('{"m":"[Revenue]"}')}]}

    out = tmp_path / "review.html"
    render_safe_refactor_html(
        plan_doc=plan_doc,
        sm_before=sm_before,
        sm_after=sm_after,
        reports_before={"rep1": rep_before},
        reports_after={"rep1": rep_after},
        output_path=str(out),
        plan_path=str(tmp_path / "plan.json"),
    )

    html_text = out.read_text(encoding="utf-8")
    assert "SafeRefactor Review Packet" in html_text
    assert "model.tmdl" in html_text
    assert "report.json" in html_text


def test_render_schema_drift_html_creates_file(tmp_path):
    plan = {
        "contract_name": "orders",
        "contract_path": "contracts/orders.yaml",
        "from_version": 1,
        "proposed_version": 2,
        "finding": {"summary": "added=1", "breaking": False},
        "recommendations": [{"type": "ADD_COLUMNS", "risk": "LOW", "message": "ok"}],
        "policy": {"require_approval": True},
        "approval": {"status": "PENDING", "approved_by": None, "comment": None, "approved_ts": None},
    }

    before = "name: orders\nversion: 1\ncolumns:\n  - name: a\n    type: string\n"
    after = "name: orders\nversion: 2\ncolumns:\n  - name: a\n    type: string\n  - name: b\n    type: int\n"

    out = tmp_path / "schema.html"
    render_schema_drift_html(
        plan=plan,
        contract_before_text=before,
        contract_after_text=after,
        output_path=str(out),
        plan_path="plan.json",
    )

    html_text = out.read_text(encoding="utf-8")
    assert "Schema Drift Review Packet" in html_text
    assert "orders" in html_text
