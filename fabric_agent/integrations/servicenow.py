"""
ServiceNow Integration
======================

Create and manage change requests in ServiceNow.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from enum import Enum

import httpx
from loguru import logger


class ChangeRequestState(str, Enum):
    NEW = "new"
    ASSESS = "assess"
    AUTHORIZE = "authorize"
    SCHEDULED = "scheduled"
    IMPLEMENT = "implement"
    REVIEW = "review"
    CLOSED = "closed"
    CANCELLED = "cancelled"


class ChangeRequestRisk(str, Enum):
    HIGH = "1"
    MODERATE = "2"
    LOW = "3"


@dataclass
class ChangeRequest:
    number: str = ""
    sys_id: str = ""
    short_description: str = ""
    description: str = ""
    state: ChangeRequestState = ChangeRequestState.NEW
    risk: ChangeRequestRisk = ChangeRequestRisk.LOW
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "number": self.number,
            "sys_id": self.sys_id,
            "short_description": self.short_description,
            "state": self.state.value,
            "risk": self.risk.value,
        }


class ServiceNowClient:
    """ServiceNow client for change request management."""
    
    def __init__(
        self,
        instance: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        assignment_group: str = "Data Platform Team",
    ):
        self.instance = instance
        self.username = username
        self.password = password
        self.assignment_group = assignment_group
        self.base_url = f"https://{instance}/api/now/table"
    
    async def create_change_request(
        self,
        change: Any,
        impact_report: Any,
    ) -> str:
        """Create a change request from impact report."""
        
        # Map risk level
        risk_mapping = {
            "critical": ChangeRequestRisk.HIGH,
            "high": ChangeRequestRisk.HIGH,
            "medium": ChangeRequestRisk.MODERATE,
            "low": ChangeRequestRisk.LOW,
            "none": ChangeRequestRisk.LOW,
        }
        risk = risk_mapping.get(
            impact_report.risk_level.value if hasattr(impact_report, 'risk_level') else "low",
            ChangeRequestRisk.LOW,
        )
        
        source_name = impact_report.source_asset.name if impact_report.source_asset else "Unknown"
        
        payload = {
            "short_description": f"Fabric Change: {change.change_type} {source_name}",
            "description": self._build_description(change, impact_report),
            "assignment_group": self.assignment_group,
            "risk": risk.value,
            "type": "normal",
            "category": "Data Platform",
            "subcategory": "Microsoft Fabric",
            "justification": f"Impact: {impact_report.total_affected} assets affected",
            "implementation_plan": self._build_implementation_plan(impact_report),
            "backout_plan": "Rollback using Fabric Agent checkpoint",
            "test_plan": "Validate affected reports after deployment",
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/change_request",
                    json=payload,
                    auth=(self.username, self.password) if self.username else None,
                    timeout=30.0,
                )
                
                if response.status_code == 201:
                    data = response.json()
                    cr_number = data.get("result", {}).get("number", "")
                    logger.info(f"Created ServiceNow CR: {cr_number}")
                    return cr_number
                else:
                    logger.error(f"ServiceNow API error: {response.status_code}")
                    return ""
        except Exception as e:
            logger.error(f"ServiceNow request failed: {e}")
            return ""
    
    async def update_status(
        self,
        cr_number: str,
        state: str,
        work_notes: Optional[str] = None,
    ) -> bool:
        """Update change request status."""
        
        state_mapping = {
            "implement": "implement",
            "implemented": "review",
            "complete": "closed",
            "failed": "review",
            "cancelled": "cancelled",
        }
        
        payload = {
            "state": state_mapping.get(state, state),
        }
        if work_notes:
            payload["work_notes"] = work_notes
        
        try:
            # First get sys_id from number
            sys_id = await self._get_sys_id(cr_number)
            if not sys_id:
                return False
            
            async with httpx.AsyncClient() as client:
                response = await client.patch(
                    f"{self.base_url}/change_request/{sys_id}",
                    json=payload,
                    auth=(self.username, self.password) if self.username else None,
                    timeout=30.0,
                )
                return response.status_code == 200
        except Exception as e:
            logger.error(f"ServiceNow update failed: {e}")
            return False
    
    async def add_work_note(self, cr_number: str, note: str) -> bool:
        """Add work note to change request."""
        return await self.update_status(cr_number, "", work_notes=note)
    
    async def get_change_request(self, cr_number: str) -> Optional[ChangeRequest]:
        """Get change request details."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/change_request",
                    params={"number": cr_number},
                    auth=(self.username, self.password) if self.username else None,
                    timeout=30.0,
                )
                
                if response.status_code == 200:
                    data = response.json()
                    results = data.get("result", [])
                    if results:
                        r = results[0]
                        return ChangeRequest(
                            number=r.get("number", ""),
                            sys_id=r.get("sys_id", ""),
                            short_description=r.get("short_description", ""),
                            description=r.get("description", ""),
                            state=ChangeRequestState(r.get("state", "new")),
                        )
        except Exception as e:
            logger.error(f"ServiceNow get failed: {e}")
        return None
    
    async def _get_sys_id(self, cr_number: str) -> Optional[str]:
        """Get sys_id from CR number."""
        cr = await self.get_change_request(cr_number)
        return cr.sys_id if cr else None
    
    def _build_description(self, change: Any, impact: Any) -> str:
        """Build CR description from impact report."""
        lines = [
            "## Change Summary",
            f"- **Type:** {change.change_type}",
            f"- **Asset:** {change.asset_name}",
            f"- **Workspace:** {change.workspace_name}",
            "",
            "## Impact Analysis",
            f"- **Risk Score:** {impact.risk_score}/100",
            f"- **Total Affected:** {impact.total_affected}",
            f"- **Workspaces:** {len(impact.affected_workspaces)}",
            "",
            "## Affected Assets",
            f"- Measures: {len(impact.affected_measures)}",
            f"- Reports: {len(impact.affected_reports)}",
            f"- Notebooks: {len(impact.affected_notebooks)}",
            f"- Pipelines: {len(impact.affected_pipelines)}",
        ]
        
        if impact.warnings:
            lines.append("")
            lines.append("## Warnings")
            for w in impact.warnings:
                lines.append(f"- {w}")
        
        return "\n".join(lines)
    
    def _build_implementation_plan(self, impact: Any) -> str:
        """Build implementation plan from impact report."""
        lines = [
            "1. Create checkpoint using Fabric Agent",
            "2. Apply changes to semantic model",
            f"3. Auto-fix {impact.auto_fix_count} dependent measures",
            f"4. Manually review {impact.manual_fix_count} items",
            "5. Validate affected reports",
            "6. Monitor for errors",
        ]
        return "\n".join(lines)
