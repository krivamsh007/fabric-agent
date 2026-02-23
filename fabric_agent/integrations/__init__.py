"""
Integrations Module
===================

Enterprise integrations for the Fabric Agent:
- Slack: Rich notifications with approval buttons
- Microsoft Teams: Adaptive card notifications
- ServiceNow: Create and manage change requests
- Email: SMTP notifications

Example:
    from fabric_agent.integrations import SlackNotifier, ServiceNowClient
    
    # Send Slack notification
    slack = SlackNotifier(webhook_url="https://hooks.slack.com/...")
    await slack.notify_impact(channel="#data-platform", impact_report=report)
    
    # Create ServiceNow change request
    snow = ServiceNowClient(instance="mycompany.service-now.com")
    cr_number = await snow.create_change_request(change, impact_report)
"""

from fabric_agent.integrations.slack import SlackNotifier
from fabric_agent.integrations.servicenow import ServiceNowClient
from fabric_agent.integrations.teams import TeamsNotifier
from fabric_agent.integrations.email import EmailNotifier

__all__ = [
    "SlackNotifier",
    "ServiceNowClient",
    "TeamsNotifier",
    "EmailNotifier",
]
