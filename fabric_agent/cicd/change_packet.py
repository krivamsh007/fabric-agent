from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from loguru import logger

from fabric_agent.reporting import render_safe_refactor_html


@dataclass
class ChangePacketPaths:
    packet_dir: Path
    plan_json: Path
    approval_json: Path
    review_html: Path
    deployment_request_json: Path
    meta_json: Path


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _safe_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)


def _infer_items_from_saferefactor_plan(plan: dict[str, Any]) -> list[dict[str, str]]:
    """Infer deployment items from SafeRefactor plan impact.

    Deployment Pipelines expects: { "sourceItemId": "<uuid>", "itemType": "Report|SemanticModel|..." }
    """
    items: list[dict[str, str]] = []
    sm_id = plan.get("semantic_model_id")
    if sm_id:
        items.append({"sourceItemId": sm_id, "itemType": "SemanticModel"})

    impact = plan.get("impact") or {}
    reports_changed = impact.get("reports_changed") or {}
    for report_id in reports_changed.keys():
        items.append({"sourceItemId": report_id, "itemType": "Report"})

    # de-dupe while preserving order
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for it in items:
        key = (it["sourceItemId"], it["itemType"])
        if key not in seen:
            seen.add(key)
            out.append(it)
    return out


def create_change_packet_from_plan(
    plan_path: str | Path,
    *,
    out_dir: str | Path = "change_packets",
    deployment_pipeline_id: str,
    source_stage_id: str,
    target_stage_id: str,
    note: str | None = None,
    include_items: bool = True,
) -> ChangePacketPaths:
    """Create a PR-friendly change packet folder.

    This is Option 2's core artifact:
    - reviewers approve via PR
    - CI reads deployment_request.json and deploys stage content
    """
    plan_path = Path(plan_path)
    plan = _read_json(plan_path)
    op_id = str(plan.get("operation_id") or plan_path.stem)

    packet_dir = Path(out_dir) / op_id
    packet_dir.mkdir(parents=True, exist_ok=True)

    # 1) Copy plan.json
    plan_json = packet_dir / "plan.json"
    _write_json(plan_json, plan)

    # 2) Approval record (even if pending)
    approval = plan.get("approval") or {"status": "PENDING"}
    approval_json = packet_dir / "approval.json"
    _write_json(approval_json, approval)

    # 3) HTML review packet (render from artifacts if present)
    review_html = packet_dir / "review.html"
    artifacts_dir = plan.get("artifacts_dir")
    if artifacts_dir and Path(artifacts_dir).exists():
        try:
            adir = Path(artifacts_dir)
            sm_before = _read_json(adir / "sm_before.json")
            sm_after = _read_json(adir / "sm_after.json")
            rep_before = _read_json(adir / "reports_before.json")
            rep_after = _read_json(adir / "reports_after.json")
            render_safe_refactor_html(
                plan_doc=plan,
                sm_before=sm_before,
                sm_after=sm_after,
                reports_before=rep_before,
                reports_after=rep_after,
                output_path=review_html.as_posix(),
                plan_path=plan_json.as_posix(),
            )
        except Exception as e:
            logger.warning("Failed to render review.html from artifacts_dir: {}", e)
            review_html.write_text(
                "<html><body><h2>Review packet unavailable</h2><p>Could not render HTML from artifacts.</p></body></html>",
                encoding="utf-8",
            )
    else:
        review_html.write_text(
            "<html><body><h2>Review packet unavailable</h2>"
            "<p>This plan did not include artifacts_dir. Re-run saferefactor plan with artifacts enabled.</p></body></html>",
            encoding="utf-8",
        )

    # 4) Deployment request (what CI executes)
    items: list[dict[str, str]] | None = None
    if include_items:
        # currently only SafeRefactor produces Fabric item list automatically
        if plan.get("kind") == "SafeRefactorPlan" or ("semantic_model_id" in plan and "old" in plan and "new" in plan):
            items = _infer_items_from_saferefactor_plan(plan)

    deployment_req = {
        "deploymentPipelineId": deployment_pipeline_id,
        "sourceStageId": source_stage_id,
        "targetStageId": target_stage_id,
        "items": items,  # may be None -> deploy all supported items
        "note": note or f"ChangePacket {op_id} (approved via PR)",
        "createdAt": _utc_now_iso(),
        "packetId": op_id,
    }
    # remove items if None to indicate "deploy all"
    if deployment_req["items"] is None:
        deployment_req.pop("items", None)

    deployment_request_json = packet_dir / "deployment_request.json"
    _write_json(deployment_request_json, deployment_req)

    meta_json = packet_dir / "meta.json"
    _write_json(
        meta_json,
        {
            "packetId": op_id,
            "createdAt": _utc_now_iso(),
            "sourcePlan": plan_path.as_posix(),
            "contains": ["plan.json", "approval.json", "review.html", "deployment_request.json"],
        },
    )

    return ChangePacketPaths(
        packet_dir=packet_dir,
        plan_json=plan_json,
        approval_json=approval_json,
        review_html=review_html,
        deployment_request_json=deployment_request_json,
        meta_json=meta_json,
    )


def _run_git(args: list[str], *, cwd: Path) -> None:
    logger.info("git {}", " ".join(args))
    res = subprocess.run(["git"] + args, cwd=cwd.as_posix(), capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"git failed: {' '.join(args)}\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}")


def git_commit_change_packet(
    *,
    repo_root: str | Path,
    packet_dir: str | Path,
    branch: str,
    message: str,
    push: bool = False,
    remote: str = "origin",
) -> None:
    """Create a PR branch and commit the change packet.

    Review + approval happens in GitHub/Azure DevOps PR. After merge, CI deploys.
    """
    repo_root = Path(repo_root)
    packet_dir = Path(packet_dir)
    if not (repo_root / ".git").exists():
        raise RuntimeError("Not a git repo. Run: git init && git add . && git commit -m 'init'")

    _run_git(["checkout", "-b", branch], cwd=repo_root)
    _run_git(["add", packet_dir.as_posix()], cwd=repo_root)
    _run_git(["commit", "-m", message], cwd=repo_root)
    if push:
        _run_git(["push", "-u", remote, branch], cwd=repo_root)
