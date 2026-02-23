"""
Self-Healing Fabric Infrastructure
===================================

Autonomous detection and remediation loop for Microsoft Fabric.

WHAT:
    Continuously scans workspaces for anomalies (broken shortcuts,
    schema drift, orphaned assets, stale tables) and applies safe
    auto-fixes where policy permits.

WHY (FAANG PATTERN):
    At scale (1000s of Fabric items), manual monitoring is impossible.
    Self-healing infra is the standard at Google (Borg), Meta (Tupperware),
    Netflix (Chaos Monkey) — this brings the same pattern to Fabric.

COMPONENTS:
    - models.py     → Anomaly, HealingPlan, HealAction, HealthReport
    - detector.py   → AnomalyDetector (wraps lineage + drift detection)
    - healer.py     → SelfHealer (applies safe fixes per anomaly type)
    - monitor.py    → SelfHealingMonitor (orchestration loop)

USAGE:
    monitor = SelfHealingMonitor(client=client, memory=memory_manager)
    report = await monitor.run_once(workspace_ids=["ws-123"], dry_run=True)
    print(f"Found {report.anomalies_found} anomalies, auto-healed {report.auto_healed}")
"""

from fabric_agent.healing.models import (
    AnomalyType,
    Anomaly,
    HealAction,
    HealActionStatus,
    HealingPlan,
    HealingResult,
    HealthReport,
)
from fabric_agent.healing.detector import AnomalyDetector
from fabric_agent.healing.healer import SelfHealer
from fabric_agent.healing.monitor import SelfHealingMonitor

__all__ = [
    # Models
    "AnomalyType",
    "Anomaly",
    "HealAction",
    "HealActionStatus",
    "HealingPlan",
    "HealingResult",
    "HealthReport",
    # Components
    "AnomalyDetector",
    "SelfHealer",
    "SelfHealingMonitor",
]
