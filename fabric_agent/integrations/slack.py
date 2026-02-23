"""
Slack Integration
=================

Send rich notifications to Slack channels with:
- Impact summaries
- Risk badges
- Approval buttons
- Links to review packets

Configuration:
    Set SLACK_WEBHOOK_URL in .env or pass directly.

Example:
    notifier = SlackNotifier(webhook_url="https://hooks.slack.com/...")
    
    # Notify about impact
    await notifier.notify_impact("#data-platform", impact_report)
    
    # Request approval
    await notifier.request_approval("#approvals", change_request)
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx
from loguru import logger


@dataclass
class SlackMessage:
    """A Slack message with blocks."""
    channel: str
    text: str
    blocks: List[Dict[str, Any]]
    thread_ts: Optional[str] = None


class SlackNotifier:
    """
    Slack notification service for Fabric Agent.
    
    Sends rich Block Kit messages to Slack channels.
    """
    
    def __init__(
        self,
        webhook_url: Optional[str] = None,
        bot_token: Optional[str] = None,
        default_channel: str = "#fabric-agent",
    ):
        """
        Initialize Slack notifier.
        
        Args:
            webhook_url: Incoming webhook URL (for simple notifications)
            bot_token: Bot token (for interactive messages)
            default_channel: Default channel to post to
        """
        self.webhook_url = webhook_url
        self.bot_token = bot_token
        self.default_channel = default_channel
    
    async def notify_impact(
        self,
        channel: Optional[str] = None,
        impact_report: Any = None,
        thread_ts: Optional[str] = None,
    ) -> bool:
        """
        Send impact analysis notification.
        
        Args:
            channel: Slack channel (default: default_channel)
            impact_report: ImpactReport object
            thread_ts: Thread timestamp for replies
        
        Returns:
            True if sent successfully
        """
        if impact_report is None:
            return False
        
        channel = channel or self.default_channel
        blocks = self._build_impact_blocks(impact_report)
        
        return await self._send_message(
            channel=channel,
            text=f"Impact Analysis: {impact_report.source_asset.name if impact_report.source_asset else 'Unknown'}",
            blocks=blocks,
            thread_ts=thread_ts,
        )
    
    async def request_approval(
        self,
        channel: Optional[str] = None,
        plan: Any = None,
        approvers: Optional[List[str]] = None,
    ) -> bool:
        """
        Send approval request notification.
        
        Args:
            channel: Slack channel
            plan: FixPlan or change plan
            approvers: List of approver Slack IDs
        
        Returns:
            True if sent successfully
        """
        if plan is None:
            return False
        
        channel = channel or self.default_channel
        blocks = self._build_approval_blocks(plan, approvers)
        
        return await self._send_message(
            channel=channel,
            text=f"Approval Request: {plan.source_change}",
            blocks=blocks,
        )
    
    async def notify_completion(
        self,
        channel: Optional[str] = None,
        result: Any = None,
        thread_ts: Optional[str] = None,
    ) -> bool:
        """
        Send completion notification.
        
        Args:
            channel: Slack channel
            result: FixResult or execution result
            thread_ts: Thread timestamp for replies
        
        Returns:
            True if sent successfully
        """
        if result is None:
            return False
        
        channel = channel or self.default_channel
        blocks = self._build_completion_blocks(result)
        
        return await self._send_message(
            channel=channel,
            text=f"Change Complete: {'Success' if result.success else 'Failed'}",
            blocks=blocks,
            thread_ts=thread_ts,
        )
    
    def _build_impact_blocks(self, report: Any) -> List[Dict[str, Any]]:
        """Build Slack blocks for impact report."""
        risk_emoji = {
            "none": "🟢",
            "low": "🟡",
            "medium": "🟠",
            "high": "🔴",
            "critical": "⛔",
        }
        
        emoji = risk_emoji.get(report.risk_level.value, "❓")
        source_name = report.source_asset.name if report.source_asset else "Unknown"
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🔍 Impact Analysis",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Source:*\n`{source_name}`",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Change:*\n{report.change_type}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Risk Level:*\n{emoji} {report.risk_level.value.upper()}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Risk Score:*\n{report.risk_score}/100",
                    },
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*📊 Impact Summary*",
                },
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Total Affected:* {report.total_affected}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Workspaces:* {len(report.affected_workspaces)}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Auto-Fixable:* {report.auto_fix_count}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Manual Required:* {report.manual_fix_count}",
                    },
                ],
            },
        ]
        
        # Add warnings if present
        if report.warnings:
            warning_text = "\n".join(f"• {w}" for w in report.warnings[:3])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*⚠️ Warnings:*\n{warning_text}",
                },
            })
        
        # Add action buttons
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "View Full Report",
                        "emoji": True,
                    },
                    "style": "primary",
                    "action_id": "view_report",
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Create Fix Plan",
                        "emoji": True,
                    },
                    "action_id": "create_fix_plan",
                },
            ],
        })
        
        return blocks
    
    def _build_approval_blocks(
        self,
        plan: Any,
        approvers: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Build Slack blocks for approval request."""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🔐 Approval Required",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Change:* {plan.source_change}\n*From:* `{plan.old_value}` → `{plan.new_value}`",
                },
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Actions:* {len(plan.actions)}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Auto-Fix:* {plan.auto_fix_count}",
                    },
                ],
            },
        ]
        
        if approvers:
            approver_mentions = " ".join(f"<@{a}>" for a in approvers)
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Approvers:* {approver_mentions}",
                },
            })
        
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "✅ Approve",
                        "emoji": True,
                    },
                    "style": "primary",
                    "action_id": "approve_plan",
                    "value": plan.plan_id,
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "❌ Reject",
                        "emoji": True,
                    },
                    "style": "danger",
                    "action_id": "reject_plan",
                    "value": plan.plan_id,
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "📄 View Details",
                        "emoji": True,
                    },
                    "action_id": "view_plan",
                    "value": plan.plan_id,
                },
            ],
        })
        
        return blocks
    
    def _build_completion_blocks(self, result: Any) -> List[Dict[str, Any]]:
        """Build Slack blocks for completion notification."""
        emoji = "✅" if result.success else "❌"
        status = "Completed Successfully" if result.success else "Failed"
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} Change {status}",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Completed:* {result.completed}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Failed:* {result.failed}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Skipped:* {result.skipped}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Duration:* {result.duration_seconds}s",
                    },
                ],
            },
        ]
        
        if result.errors:
            error_text = "\n".join(f"• {e}" for e in result.errors[:3])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Errors:*\n{error_text}",
                },
            })
        
        return blocks
    
    async def _send_message(
        self,
        channel: str,
        text: str,
        blocks: List[Dict[str, Any]],
        thread_ts: Optional[str] = None,
    ) -> bool:
        """Send message to Slack."""
        if self.webhook_url:
            return await self._send_webhook(text, blocks)
        elif self.bot_token:
            return await self._send_api(channel, text, blocks, thread_ts)
        else:
            logger.warning("No Slack credentials configured")
            return False
    
    async def _send_webhook(
        self,
        text: str,
        blocks: List[Dict[str, Any]],
    ) -> bool:
        """Send via incoming webhook."""
        payload = {
            "text": text,
            "blocks": blocks,
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url,
                    json=payload,
                    timeout=10.0,
                )
                return response.status_code == 200
        except Exception as e:
            logger.error(f"Slack webhook failed: {e}")
            return False
    
    async def _send_api(
        self,
        channel: str,
        text: str,
        blocks: List[Dict[str, Any]],
        thread_ts: Optional[str] = None,
    ) -> bool:
        """Send via Slack API."""
        payload = {
            "channel": channel,
            "text": text,
            "blocks": blocks,
        }
        if thread_ts:
            payload["thread_ts"] = thread_ts
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    "https://slack.com/api/chat.postMessage",
                    json=payload,
                    headers={"Authorization": f"Bearer {self.bot_token}"},
                    timeout=10.0,
                )
                data = response.json()
                return data.get("ok", False)
        except Exception as e:
            logger.error(f"Slack API failed: {e}")
            return False
