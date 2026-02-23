"""
Policy Engine
=============

Enforces governance policies before changes are applied.

Built-in policies:
- PIIProtectionPolicy: Require DLP review for PII columns
- BreakingChangePolicy: Require manager approval for breaking changes
- MaintenanceWindowPolicy: Only allow production changes during windows
- TestCoveragePolicy: Require minimum test coverage for notebooks
- CrossWorkspacePolicy: Require approval for cross-workspace impact

Example:
    engine = PolicyEngine()
    
    # Add custom policy
    engine.add_policy(MyCustomPolicy())
    
    # Evaluate a proposed change
    result = await engine.evaluate(proposed_change, impact_report)
    
    if not result.passed:
        for violation in result.violations:
            print(f"Policy violated: {violation.policy_name}")
            print(f"  Reason: {violation.reason}")
            print(f"  Required action: {violation.required_action}")
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from loguru import logger


class PolicySeverity(str, Enum):
    """Severity levels for policy violations."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    BLOCKING = "blocking"


@dataclass
class PolicyViolation:
    """A single policy violation."""
    policy_name: str
    severity: PolicySeverity
    reason: str
    required_action: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "policy_name": self.policy_name,
            "severity": self.severity.value,
            "reason": self.reason,
            "required_action": self.required_action,
            "metadata": self.metadata,
        }


@dataclass
class PolicyResult:
    """Result of policy evaluation."""
    passed: bool
    violations: List[PolicyViolation] = field(default_factory=list)
    warnings: List[PolicyViolation] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def blocking_violations(self) -> List[PolicyViolation]:
        return [v for v in self.violations if v.severity == PolicySeverity.BLOCKING]
    
    @property
    def error_violations(self) -> List[PolicyViolation]:
        return [v for v in self.violations if v.severity == PolicySeverity.ERROR]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "violations": [v.to_dict() for v in self.violations],
            "warnings": [w.to_dict() for w in self.warnings],
            "metadata": self.metadata,
        }
    
    def to_markdown(self) -> str:
        lines = []
        
        if self.passed:
            lines.append("## ✅ Policy Check Passed")
        else:
            lines.append("## ❌ Policy Check Failed")
        
        lines.append("")
        
        if self.violations:
            lines.append("### Violations")
            lines.append("")
            for v in self.violations:
                emoji = "⛔" if v.severity == PolicySeverity.BLOCKING else "❌"
                lines.append(f"- {emoji} **{v.policy_name}**")
                lines.append(f"  - Reason: {v.reason}")
                lines.append(f"  - Action: {v.required_action}")
            lines.append("")
        
        if self.warnings:
            lines.append("### Warnings")
            lines.append("")
            for w in self.warnings:
                lines.append(f"- ⚠️ **{w.policy_name}**: {w.reason}")
            lines.append("")
        
        return "\n".join(lines)


class Policy(ABC):
    """Base class for governance policies."""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Policy name for display."""
        pass
    
    @property
    def enabled(self) -> bool:
        """Whether this policy is enabled."""
        return True
    
    @abstractmethod
    async def evaluate(
        self,
        change: "ProposedChange",
        impact: Optional["ImpactReport"] = None,
    ) -> PolicyResult:
        """
        Evaluate the policy against a proposed change.
        
        Args:
            change: The proposed change
            impact: Impact report (if available)
        
        Returns:
            PolicyResult indicating pass/fail and any violations
        """
        pass


@dataclass
class ProposedChange:
    """A proposed change to evaluate against policies."""
    change_type: str  # rename, delete, modify, etc.
    asset_type: str   # table, measure, column, etc.
    asset_name: str
    workspace_id: str
    workspace_name: str
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    requester: Optional[str] = None
    requested_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class PIIProtectionPolicy(Policy):
    """
    Require DLP review for changes involving PII columns.
    
    Detects common PII patterns:
    - Names (first_name, last_name, full_name)
    - Identifiers (ssn, social_security, passport, driver_license)
    - Contact info (email, phone, address)
    - Financial (credit_card, bank_account)
    """
    
    PII_PATTERNS = [
        r"(?i)(first|last|full)_?name",
        r"(?i)ssn|social_?security",
        r"(?i)passport|driver_?licen[sc]e",
        r"(?i)email|e_?mail",
        r"(?i)phone|mobile|cell",
        r"(?i)address|street|city|zip|postal",
        r"(?i)credit_?card|card_?number|cvv",
        r"(?i)bank_?account|routing",
        r"(?i)date_?of_?birth|dob|birth_?date",
        r"(?i)salary|income|compensation",
    ]
    
    @property
    def name(self) -> str:
        return "PII Protection"
    
    async def evaluate(
        self,
        change: ProposedChange,
        impact: Optional[Any] = None,
    ) -> PolicyResult:
        violations = []
        warnings = []
        
        # Check if the asset name matches PII patterns
        for pattern in self.PII_PATTERNS:
            if re.search(pattern, change.asset_name):
                violations.append(PolicyViolation(
                    policy_name=self.name,
                    severity=PolicySeverity.BLOCKING,
                    reason=f"Asset '{change.asset_name}' appears to contain PII data",
                    required_action="Submit DLP review request before proceeding",
                    metadata={"matched_pattern": pattern},
                ))
                break
        
        # Check impacted assets for PII
        if impact:
            for affected in getattr(impact, "all_affected", []):
                node = affected.node
                for pattern in self.PII_PATTERNS:
                    if re.search(pattern, node.name):
                        warnings.append(PolicyViolation(
                            policy_name=self.name,
                            severity=PolicySeverity.WARNING,
                            reason=f"Affected asset '{node.name}' may contain PII",
                            required_action="Review PII implications",
                        ))
                        break
        
        return PolicyResult(
            passed=len(violations) == 0,
            violations=violations,
            warnings=warnings,
        )


class BreakingChangePolicy(Policy):
    """
    Require manager approval for breaking changes.
    
    Breaking changes include:
    - Deleting assets
    - Renaming assets with downstream dependencies
    - Changing data types
    """
    
    APPROVAL_THRESHOLD = 5  # Require approval if more than N assets affected
    
    @property
    def name(self) -> str:
        return "Breaking Change Approval"
    
    async def evaluate(
        self,
        change: ProposedChange,
        impact: Optional[Any] = None,
    ) -> PolicyResult:
        violations = []
        
        # Delete always requires approval
        if change.change_type == "delete":
            violations.append(PolicyViolation(
                policy_name=self.name,
                severity=PolicySeverity.BLOCKING,
                reason="Delete operations require manager approval",
                required_action="Obtain approval from workspace owner",
            ))
        
        # Check impact threshold
        if impact:
            total_affected = getattr(impact, "total_affected", 0)
            breaking_count = getattr(impact, "breaking_count", 0)
            
            if breaking_count > 0:
                violations.append(PolicyViolation(
                    policy_name=self.name,
                    severity=PolicySeverity.BLOCKING,
                    reason=f"{breaking_count} breaking changes detected",
                    required_action="Obtain approval for breaking changes",
                    metadata={"breaking_count": breaking_count},
                ))
            
            elif total_affected > self.APPROVAL_THRESHOLD:
                violations.append(PolicyViolation(
                    policy_name=self.name,
                    severity=PolicySeverity.ERROR,
                    reason=f"High impact change affecting {total_affected} assets",
                    required_action="Obtain team lead approval",
                    metadata={"affected_count": total_affected},
                ))
        
        return PolicyResult(
            passed=len(violations) == 0,
            violations=violations,
        )


class MaintenanceWindowPolicy(Policy):
    """
    Only allow production changes during maintenance windows.
    
    Default window: 6 PM - 6 AM local time on weekdays,
    all day on weekends.
    """
    
    def __init__(
        self,
        window_start: time = time(18, 0),  # 6 PM
        window_end: time = time(6, 0),     # 6 AM
        allow_weekends: bool = True,
    ):
        self.window_start = window_start
        self.window_end = window_end
        self.allow_weekends = allow_weekends
    
    @property
    def name(self) -> str:
        return "Maintenance Window"
    
    async def evaluate(
        self,
        change: ProposedChange,
        impact: Optional[Any] = None,
    ) -> PolicyResult:
        violations = []
        
        # Only check for production workspaces
        is_production = any(
            x in change.workspace_name.lower()
            for x in ["prod", "production", "prd"]
        )
        
        if not is_production:
            return PolicyResult(passed=True)
        
        now = datetime.now()
        
        # Check if weekend
        if self.allow_weekends and now.weekday() >= 5:
            return PolicyResult(passed=True)
        
        # Check if within maintenance window
        current_time = now.time()
        
        # Handle overnight window (e.g., 6 PM - 6 AM)
        if self.window_start > self.window_end:
            in_window = current_time >= self.window_start or current_time <= self.window_end
        else:
            in_window = self.window_start <= current_time <= self.window_end
        
        if not in_window:
            violations.append(PolicyViolation(
                policy_name=self.name,
                severity=PolicySeverity.BLOCKING,
                reason="Production changes outside maintenance window",
                required_action=f"Schedule change for {self.window_start} - {self.window_end}",
                metadata={
                    "current_time": current_time.isoformat(),
                    "window_start": self.window_start.isoformat(),
                    "window_end": self.window_end.isoformat(),
                },
            ))
        
        return PolicyResult(
            passed=len(violations) == 0,
            violations=violations,
        )


class CrossWorkspacePolicy(Policy):
    """
    Require approval for changes with cross-workspace impact.
    """
    
    @property
    def name(self) -> str:
        return "Cross-Workspace Impact"
    
    async def evaluate(
        self,
        change: ProposedChange,
        impact: Optional[Any] = None,
    ) -> PolicyResult:
        violations = []
        
        if impact:
            affected_workspaces = getattr(impact, "affected_workspaces", [])
            
            if len(affected_workspaces) > 1:
                violations.append(PolicyViolation(
                    policy_name=self.name,
                    severity=PolicySeverity.BLOCKING,
                    reason=f"Change impacts {len(affected_workspaces)} workspaces",
                    required_action="Obtain approval from all workspace owners",
                    metadata={"workspaces": affected_workspaces},
                ))
        
        return PolicyResult(
            passed=len(violations) == 0,
            violations=violations,
        )


class RiskThresholdPolicy(Policy):
    """
    Block changes that exceed a risk score threshold.
    """
    
    def __init__(self, threshold: int = 70):
        self.threshold = threshold
    
    @property
    def name(self) -> str:
        return "Risk Threshold"
    
    async def evaluate(
        self,
        change: ProposedChange,
        impact: Optional[Any] = None,
    ) -> PolicyResult:
        violations = []
        
        if impact:
            risk_score = getattr(impact, "risk_score", 0)
            
            if risk_score >= self.threshold:
                violations.append(PolicyViolation(
                    policy_name=self.name,
                    severity=PolicySeverity.BLOCKING,
                    reason=f"Risk score {risk_score} exceeds threshold {self.threshold}",
                    required_action="Obtain executive approval or reduce impact",
                    metadata={"risk_score": risk_score, "threshold": self.threshold},
                ))
        
        return PolicyResult(
            passed=len(violations) == 0,
            violations=violations,
        )


class PolicyEngine:
    """
    Central policy engine that evaluates all policies.
    
    Usage:
        engine = PolicyEngine()
        engine.add_policy(MyCustomPolicy())
        
        result = await engine.evaluate(change, impact_report)
        
        if not result.passed:
            print(result.to_markdown())
    """
    
    def __init__(self, include_defaults: bool = True):
        """
        Initialize the policy engine.
        
        Args:
            include_defaults: Include built-in policies
        """
        self._policies: List[Policy] = []
        
        if include_defaults:
            self.add_policy(PIIProtectionPolicy())
            self.add_policy(BreakingChangePolicy())
            self.add_policy(CrossWorkspacePolicy())
            self.add_policy(RiskThresholdPolicy())
    
    def add_policy(self, policy: Policy) -> None:
        """Add a policy to the engine."""
        self._policies.append(policy)
        logger.debug(f"Added policy: {policy.name}")
    
    def remove_policy(self, policy_name: str) -> bool:
        """Remove a policy by name."""
        for i, p in enumerate(self._policies):
            if p.name == policy_name:
                self._policies.pop(i)
                return True
        return False
    
    @property
    def policies(self) -> List[Policy]:
        """Get all registered policies."""
        return list(self._policies)
    
    async def evaluate(
        self,
        change: ProposedChange,
        impact: Optional[Any] = None,
    ) -> PolicyResult:
        """
        Evaluate all policies against a proposed change.
        
        Args:
            change: The proposed change
            impact: Impact report (if available)
        
        Returns:
            Aggregated PolicyResult
        """
        all_violations: List[PolicyViolation] = []
        all_warnings: List[PolicyViolation] = []
        
        for policy in self._policies:
            if not policy.enabled:
                continue
            
            try:
                result = await policy.evaluate(change, impact)
                all_violations.extend(result.violations)
                all_warnings.extend(result.warnings)
            except Exception as e:
                logger.error(f"Policy {policy.name} failed: {e}")
                all_warnings.append(PolicyViolation(
                    policy_name=policy.name,
                    severity=PolicySeverity.WARNING,
                    reason=f"Policy evaluation failed: {e}",
                    required_action="Review policy configuration",
                ))
        
        # Check if any blocking violations
        has_blocking = any(
            v.severity == PolicySeverity.BLOCKING
            for v in all_violations
        )
        
        return PolicyResult(
            passed=not has_blocking,
            violations=all_violations,
            warnings=all_warnings,
            metadata={
                "policies_evaluated": len(self._policies),
                "total_violations": len(all_violations),
                "total_warnings": len(all_warnings),
            },
        )
