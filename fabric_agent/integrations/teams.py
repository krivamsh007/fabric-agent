"""
Microsoft Teams Integration
===========================

Send adaptive card notifications to Teams channels.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx
from loguru import logger


class TeamsNotifier:
    """Microsoft Teams notification service."""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
    
    async def notify_impact(
        self,
        impact_report: Any,
    ) -> bool:
        """Send impact analysis notification."""
        card = self._build_impact_card(impact_report)
        return await self._send_card(card)
    
    async def request_approval(
        self,
        plan: Any,
    ) -> bool:
        """Send approval request."""
        card = self._build_approval_card(plan)
        return await self._send_card(card)
    
    def _build_impact_card(self, report: Any) -> Dict[str, Any]:
        """Build adaptive card for impact report."""
        risk_colors = {
            "none": "good",
            "low": "good",
            "medium": "warning",
            "high": "attention",
            "critical": "attention",
        }
        
        source_name = report.source_asset.name if report.source_asset else "Unknown"
        color = risk_colors.get(report.risk_level.value, "default")
        
        return {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "size": "Large",
                            "weight": "Bolder",
                            "text": "🔍 Impact Analysis",
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Source", "value": source_name},
                                {"title": "Change", "value": report.change_type},
                                {"title": "Risk", "value": f"{report.risk_level.value.upper()} ({report.risk_score}/100)"},
                                {"title": "Affected", "value": str(report.total_affected)},
                            ],
                        },
                        {
                            "type": "ColumnSet",
                            "columns": [
                                {
                                    "type": "Column",
                                    "items": [{"type": "TextBlock", "text": f"📐 Measures: {len(report.affected_measures)}"}],
                                },
                                {
                                    "type": "Column",
                                    "items": [{"type": "TextBlock", "text": f"📈 Reports: {len(report.affected_reports)}"}],
                                },
                            ],
                        },
                    ],
                    "actions": [
                        {
                            "type": "Action.OpenUrl",
                            "title": "View Full Report",
                            "url": "https://fabric.microsoft.com",
                        },
                    ],
                },
            }],
        }
    
    def _build_approval_card(self, plan: Any) -> Dict[str, Any]:
        """Build adaptive card for approval request."""
        return {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "size": "Large",
                            "weight": "Bolder",
                            "text": "🔐 Approval Required",
                        },
                        {
                            "type": "TextBlock",
                            "text": f"Change: {plan.source_change}",
                        },
                        {
                            "type": "TextBlock",
                            "text": f"`{plan.old_value}` → `{plan.new_value}`",
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Actions", "value": str(len(plan.actions))},
                                {"title": "Auto-Fix", "value": str(plan.auto_fix_count)},
                                {"title": "Manual", "value": str(plan.manual_count)},
                            ],
                        },
                    ],
                    "actions": [
                        {
                            "type": "Action.Submit",
                            "title": "✅ Approve",
                            "data": {"action": "approve", "plan_id": plan.plan_id},
                        },
                        {
                            "type": "Action.Submit",
                            "title": "❌ Reject",
                            "data": {"action": "reject", "plan_id": plan.plan_id},
                        },
                    ],
                },
            }],
        }
    
    async def _send_card(self, card: Dict[str, Any]) -> bool:
        """Send adaptive card to Teams."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url,
                    json=card,
                    timeout=10.0,
                )
                return response.status_code == 200
        except Exception as e:
            logger.error(f"Teams notification failed: {e}")
            return False
