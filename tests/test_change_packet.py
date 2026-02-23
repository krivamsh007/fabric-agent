import json
from pathlib import Path

from fabric_agent.cicd.change_packet import create_change_packet_from_plan


def test_create_change_packet_infers_items(tmp_path: Path):
    # Build a minimal SafeRefactor-like plan with artifacts.
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    (artifacts / "sm_before.json").write_text(json.dumps({"parts": []}), encoding="utf-8")
    (artifacts / "sm_after.json").write_text(json.dumps({"parts": []}), encoding="utf-8")
    (artifacts / "reports_before.json").write_text(json.dumps({}), encoding="utf-8")
    (artifacts / "reports_after.json").write_text(json.dumps({}), encoding="utf-8")

    plan = {
        "kind": "SafeRefactorPlan",
        "operation_id": "op123",
        "workspace_id": "ws1",
        "semantic_model_id": "11111111-1111-1111-1111-111111111111",
        "old": "[A]",
        "new": "[B]",
        "impact": {"reports_changed": {"22222222-2222-2222-2222-222222222222": ["definition.pbix"]}, "risk": "LOW"},
        "artifacts_dir": artifacts.as_posix(),
        "approval": {"status": "PENDING"},
    }
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    out_dir = tmp_path / "change_packets"
    paths = create_change_packet_from_plan(
        plan_path,
        out_dir=out_dir,
        deployment_pipeline_id="pipe",
        source_stage_id="dev",
        target_stage_id="test",
        note="hello",
        include_items=True,
    )

    assert paths.packet_dir.exists()
    req = json.loads(paths.deployment_request_json.read_text(encoding="utf-8"))
    assert req["deploymentPipelineId"] == "pipe"
    assert req["sourceStageId"] == "dev"
    assert req["targetStageId"] == "test"
    assert req["note"] == "hello"

    # Inferred item list contains semantic model + report
    items = req["items"]
    assert {"sourceItemId": plan["semantic_model_id"], "itemType": "SemanticModel"} in items
    assert {"sourceItemId": "22222222-2222-2222-2222-222222222222", "itemType": "Report"} in items

    # Review html exists
    assert paths.review_html.exists()
