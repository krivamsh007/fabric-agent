"""
Schema Drift Edge Case Tests
=============================

Verifies that detect_drift() correctly classifies type changes,
nullability changes, column additions, and column removals.

Key safety properties tested:
  - Type changes (int→string, string→int) are always breaking
  - Nullability changes in either direction are breaking
  - Empty observed schema → all columns removed → breaking
  - Empty contract → all columns added → non-breaking
  - Multiple combined changes → breaking if any single change is breaking
  - Identical schema → no drift detected
"""
from __future__ import annotations

import pytest

from fabric_agent.schema.contracts import ColumnContract, DataContract
from fabric_agent.schema.drift import detect_drift, DriftFinding


# =============================================================================
# Helpers
# =============================================================================

def _contract(*columns: tuple[str, str, bool]) -> DataContract:
    """Create a DataContract from (name, type, nullable) tuples."""
    return DataContract(
        name="test_contract",
        version=1,
        columns=[
            ColumnContract(name=n, type=t, nullable=nul)
            for n, t, nul in columns
        ],
    )


def _observed(*columns: tuple[str, str, bool]) -> list[dict]:
    """Create observed schema from (name, type, nullable) tuples."""
    return [{"name": n, "type": t, "nullable": nul} for n, t, nul in columns]


# =============================================================================
# Tests — Type changes
# =============================================================================


@pytest.mark.unit
def test_type_change_int_to_string_is_breaking():
    """Column 'amount' type int→string → breaking=True."""
    contract = _contract(("amount", "int", True))
    observed = _observed(("amount", "string", True))

    finding = detect_drift(contract, observed)

    assert finding.breaking is True
    assert len(finding.type_changed) == 1
    assert finding.type_changed[0]["name"] == "amount"
    assert finding.type_changed[0]["from"] == "int"
    assert finding.type_changed[0]["to"] == "string"


@pytest.mark.unit
def test_type_change_string_to_int_is_breaking():
    """Column 'id' type string→int → breaking=True."""
    contract = _contract(("id", "string", False))
    observed = _observed(("id", "int", False))

    finding = detect_drift(contract, observed)

    assert finding.breaking is True
    assert len(finding.type_changed) == 1


# =============================================================================
# Tests — Nullability changes
# =============================================================================


@pytest.mark.unit
def test_nullable_true_to_false_is_breaking():
    """Column nullable True→False → breaking=True (tightens contract)."""
    contract = _contract(("email", "string", True))
    observed = _observed(("email", "string", False))

    finding = detect_drift(contract, observed)

    assert finding.breaking is True
    assert len(finding.nullability_changed) == 1
    assert finding.nullability_changed[0]["from"] is True
    assert finding.nullability_changed[0]["to"] is False


@pytest.mark.unit
def test_nullable_false_to_true_is_breaking():
    """Column nullable False→True → breaking=True (loosens contract)."""
    contract = _contract(("customer_id", "int", False))
    observed = _observed(("customer_id", "int", True))

    finding = detect_drift(contract, observed)

    assert finding.breaking is True
    assert len(finding.nullability_changed) == 1


# =============================================================================
# Tests — Column additions and removals
# =============================================================================


@pytest.mark.unit
def test_empty_observed_schema_all_removed():
    """observed=[] → all contract columns in removed, breaking=True."""
    contract = _contract(
        ("id", "int", False),
        ("name", "string", True),
        ("amount", "decimal", True),
    )
    observed: list[dict] = []

    finding = detect_drift(contract, observed)

    assert finding.breaking is True
    assert len(finding.removed) == 3
    assert len(finding.added) == 0


@pytest.mark.unit
def test_empty_contract_all_added():
    """Contract has 0 columns, observed has 3 → added=3, breaking=False."""
    contract = _contract()  # no columns
    observed = _observed(
        ("col_a", "string", True),
        ("col_b", "int", False),
        ("col_c", "decimal", True),
    )

    finding = detect_drift(contract, observed)

    assert finding.breaking is False
    assert len(finding.added) == 3
    assert len(finding.removed) == 0
    assert len(finding.type_changed) == 0


# =============================================================================
# Tests — Combined changes
# =============================================================================


@pytest.mark.unit
def test_multiple_changes_combined():
    """1 added + 1 removed + 1 type change → breaking=True."""
    contract = _contract(
        ("id", "int", False),
        ("old_col", "string", True),
        ("amount", "int", True),
    )
    observed = _observed(
        ("id", "int", False),
        ("new_col", "string", True),  # added (old_col removed)
        ("amount", "string", True),   # type changed int→string
    )

    finding = detect_drift(contract, observed)

    assert finding.breaking is True
    assert len(finding.added) == 1  # new_col
    assert len(finding.removed) == 1  # old_col
    assert len(finding.type_changed) == 1  # amount


@pytest.mark.unit
def test_identical_schema_no_drift():
    """Contract == observed → added=0, removed=0, breaking=False."""
    contract = _contract(
        ("id", "int", False),
        ("name", "string", True),
        ("amount", "decimal", True),
    )
    observed = _observed(
        ("id", "int", False),
        ("name", "string", True),
        ("amount", "decimal", True),
    )

    finding = detect_drift(contract, observed)

    assert finding.breaking is False
    assert len(finding.added) == 0
    assert len(finding.removed) == 0
    assert len(finding.type_changed) == 0
    assert len(finding.nullability_changed) == 0
