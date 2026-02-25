"""
Operational Guards — Freshness and Maintenance
===============================================

Guards detect silent killers in Fabric infrastructure:

- ``FreshnessGuard``  — SQL Endpoint sync lag detection (tables stale with no alert)
- ``MaintenanceGuard`` — Table Maintenance pre-submission validation (prevent silent Spark failures)
- ``GuardMonitor``     — Orchestrator that runs both guards on a schedule

FAANG PARALLEL:
    Google SRE's "Defense in Depth" principle: production systems need multiple
    independent safety layers. The healing module fixes known anomalies; the guards
    module prevents *new* anomalies from going undetected by continuously monitoring
    for silent operational gaps that Fabric does not alert on natively.
"""

from fabric_agent.guards.freshness_guard import FreshnessGuard
from fabric_agent.guards.maintenance_guard import MaintenanceGuard
from fabric_agent.guards.monitor import GuardMonitor

__all__ = ["FreshnessGuard", "MaintenanceGuard", "GuardMonitor"]
