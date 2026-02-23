from __future__ import annotations

import copy
import json
import time
import uuid
from dataclasses import dataclass

from fabric_agent.core.approval import require_approved

from .contracts import ColumnContract, DataContract


@dataclass
class DriftFinding:
    added: list[dict]
    removed: list[dict]
    type_changed: list[dict]
    nullability_changed: list[dict]
    breaking: bool
    summary: str


def detect_drift(contract: DataContract, observed: list[dict]) -> DriftFinding:
    cidx = {c.name.lower(): c for c in contract.columns}
    oidx = {c["name"].lower(): c for c in observed}

    added: list[dict] = []
    removed: list[dict] = []
    type_changed: list[dict] = []
    nullability_changed: list[dict] = []

    for name, ocol in oidx.items():
        if name not in cidx:
            added.append(ocol)
        else:
            ccol = cidx[name]
            if str(ccol.type).lower() != str(ocol.get("type")).lower():
                type_changed.append({"name": ocol["name"], "from": ccol.type, "to": ocol.get("type")})
            if bool(ccol.nullable) != bool(ocol.get("nullable", True)):
                nullability_changed.append(
                    {
                        "name": ocol["name"],
                        "from": ccol.nullable,
                        "to": bool(ocol.get("nullable", True)),
                    }
                )

    for name, ccol in cidx.items():
        if name not in oidx:
            removed.append({"name": ccol.name, "type": ccol.type, "nullable": ccol.nullable})

    breaking = bool(removed or type_changed or nullability_changed)
    summary = (
        f"added={len(added)}, removed={len(removed)}, type_changed={len(type_changed)}, "
        f"nullability_changed={len(nullability_changed)}, breaking={breaking}"
    )
    return DriftFinding(added, removed, type_changed, nullability_changed, breaking, summary)


def _recommendations(f: DriftFinding) -> list[dict]:
    recs: list[dict] = []
    if f.added:
        recs.append(
            {
                "type": "ADD_COLUMNS",
                "risk": "LOW",
                "message": "Additive columns detected. Safe to update contract and downstream mappings after approval.",
            }
        )
    if f.removed:
        recs.append(
            {
                "type": "REMOVED_COLUMNS",
                "risk": "HIGH",
                "message": "Columns removed. Recommend compatibility view or versioned table to protect downstream consumers.",
            }
        )
    if f.type_changed:
        recs.append(
            {
                "type": "TYPE_CHANGE",
                "risk": "HIGH",
                "message": "Type changes detected. Recommend versioning/backfill validation before switching consumers.",
            }
        )
    if f.nullability_changed:
        recs.append(
            {
                "type": "NULLABILITY_CHANGE",
                "risk": "MEDIUM",
                "message": "Nullability changed. Recommend profiling and staged rollout for strict checks.",
            }
        )
    if not recs:
        recs.append({"type": "NO_DRIFT", "risk": "LOW", "message": "No schema drift detected."})
    return recs


def simulate_proposed_contract(contract: DataContract, plan: dict) -> DataContract:
    """Simulate the contract after applying a plan without writing anything to disk."""
    contract2: DataContract = copy.deepcopy(contract)
    finding = plan.get("finding", {})

    for c in finding.get("added", []) or []:
        contract2.columns.append(
            ColumnContract(
                name=c["name"],
                type=c.get("type", "string"),
                nullable=bool(c.get("nullable", True)),
                description="(added by schema drift agent)",
                tags=["auto-added"],
            )
        )

    if finding.get("added") or finding.get("breaking"):
        contract2.version = int(plan.get("proposed_version", contract2.version + 1))

    return contract2


def build_plan(contract_path: str, observed_schema_path: str, *, output_path: str | None = None) -> dict:
    contract = DataContract.load(contract_path)
    observed = json.loads(open(observed_schema_path, "r", encoding="utf-8").read())
    finding = detect_drift(contract, observed)

    op_id = f"schema_{uuid.uuid4().hex[:12]}"

    plan = {
        "operation_id": op_id,
        "ts": int(time.time()),
        "contract_path": contract_path,
        "contract_name": contract.name,
        "from_version": contract.version,
        "proposed_version": contract.version + (1 if (finding.added or finding.breaking) else 0),
        "finding": {
            "summary": finding.summary,
            "breaking": finding.breaking,
            "added": finding.added,
            "removed": finding.removed,
            "type_changed": finding.type_changed,
            "nullability_changed": finding.nullability_changed,
        },
        "recommendations": _recommendations(finding),
        "policy": {
            "auto_apply_additive": True,
            "auto_apply_breaking": False,
            "require_approval": True,
        },
        "approval": {
            "status": "PENDING",
            "approved_by": None,
            "comment": None,
            "approved_ts": None,
        },
    }
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(plan, f, indent=2)
    return plan


def apply_plan(plan_path: str) -> str:
    plan = json.loads(open(plan_path, "r", encoding="utf-8").read())

    if bool(plan.get("policy", {}).get("require_approval", True)):
        require_approved(plan)

    contract_path = plan["contract_path"]
    contract = DataContract.load(contract_path)

    finding = plan.get("finding", {})
    if finding.get("breaking") and plan.get("policy", {}).get("auto_apply_breaking") is not True:
        raise RuntimeError("Breaking drift detected; policy forbids auto-apply. Review and approve manually.")

    for c in finding.get("added", []) or []:
        contract.columns.append(
            ColumnContract(
                name=c["name"],
                type=c.get("type", "string"),
                nullable=bool(c.get("nullable", True)),
                description="(added by schema drift agent)",
                tags=["auto-added"],
            )
        )

    if finding.get("added") or finding.get("breaking"):
        contract.version = int(plan.get("proposed_version", contract.version + 1))

    contract.dump(contract_path)
    return contract_path
