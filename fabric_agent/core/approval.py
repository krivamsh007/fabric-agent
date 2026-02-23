from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


APPROVALS_DIR = Path("memory/approvals")
APPROVALS_DIR.mkdir(parents=True, exist_ok=True)


def approve_plan_file(plan_path: str, *, approver: str, comment: str | None = None) -> dict[str, Any]:
    """Stamp an approval into a plan JSON and write a standalone approval record.

    This is intentionally simple and local-file based so it works in:
    - air-gapped environments
    - PR-based change control
    - ticket attachments

    Returns the updated plan dict.
    """
    p = Path(plan_path)
    plan = json.loads(p.read_text(encoding="utf-8"))

    approval = plan.get("approval") or {}
    approval.update(
        {
            "status": "APPROVED",
            "approved_by": approver,
            "comment": comment,
            "approved_ts": int(time.time()),
        }
    )
    plan["approval"] = approval

    # Update plan file
    p.write_text(json.dumps(plan, indent=2), encoding="utf-8")

    # Also write separate approval record
    op_id = str(plan.get("operation_id") or plan.get("contract_name") or "approval")
    rec_path = APPROVALS_DIR / f"{op_id}_{approval['approved_ts']}.json"
    rec_path.write_text(json.dumps({"plan_path": str(p), "approval": approval}, indent=2), encoding="utf-8")

    return plan


def require_approved(plan: dict[str, Any]) -> None:
    approval = plan.get("approval") or {}
    if str(approval.get("status", "")).upper() != "APPROVED":
        raise RuntimeError(
            "Approval required. Generate a review packet and run: "
            "fabric-agent saferefactor approve --plan <plan.json> --approver \"NAME\""
        )
