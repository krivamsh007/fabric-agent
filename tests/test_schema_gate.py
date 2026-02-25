"""
Schema Gate Unit Tests
======================
Tests the CI/CD schema gate logic: drift detection, gate decision, contract
loading, risk classification, and report structure.

All tests are offline — no Fabric API calls required.

Run:  pytest tests/test_schema_gate.py -v
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

# Import gate helpers directly from the script
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.run_fabricops_schema_gate import (
    auto_discover_contract,
    gate_decision,
    load_contract,
    load_proposed_schema,
    required_changes,
    risk_from_drift_and_impact,
)
from fabric_agent.schema.contracts import ColumnContract, DataContract
from fabric_agent.schema.drift import detect_drift


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SCHEMAS_DIR = ROOT / "schemas"
EXAMPLES_DIR = ROOT / "examples"

FACT_SALES_CONTRACT = SCHEMAS_DIR / "fact_sales.json"
DAY2_ADDITIVE = EXAMPLES_DIR / "day2_additive.json"
DAY2_BREAKING = EXAMPLES_DIR / "day2_breaking.json"


def _load_contract_columns() -> List[Dict[str, Any]]:
    raw = json.loads(FACT_SALES_CONTRACT.read_text(encoding="utf-8"))
    return raw["columns"]


def _make_contract(columns: List[Dict[str, Any]]) -> DataContract:
    cols = [
        ColumnContract(
            name=str(c["name"]),
            type=str(c.get("type", "string")),
            nullable=bool(c.get("nullable", True)),
        )
        for c in columns
    ]
    return DataContract(name="test_table", version=1, owner="test", columns=cols)


# ---------------------------------------------------------------------------
# Gate decision tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestGateDecision:
    """Gate exit code logic."""

    def test_additive_only_passes(self):
        contract = _make_contract(_load_contract_columns())
        proposed = json.loads(DAY2_ADDITIVE.read_text(encoding="utf-8"))
        finding = detect_drift(contract, proposed)

        assert finding.breaking is False
        assert len(finding.added) == 2  # channel_code, loyalty_points

        result, code = gate_decision(
            breaking=finding.breaking,
            skip_impact=False,
            impact_analyzed=True,
            total_downstream=0,
            impact_threshold=1,
        )
        assert result == "PASS"
        assert code == 0

    def test_breaking_with_impact_fails(self):
        contract = _make_contract(_load_contract_columns())
        proposed = json.loads(DAY2_BREAKING.read_text(encoding="utf-8"))
        finding = detect_drift(contract, proposed)

        assert finding.breaking is True
        assert len(finding.removed) == 1  # discount_pct
        assert len(finding.type_changed) == 2  # sale_id, sale_date

        result, code = gate_decision(
            breaking=True,
            skip_impact=False,
            impact_analyzed=True,
            total_downstream=15,
            impact_threshold=1,
        )
        assert result == "FAIL"
        assert code == 1

    def test_breaking_below_threshold_passes(self):
        result, code = gate_decision(
            breaking=True,
            skip_impact=False,
            impact_analyzed=True,
            total_downstream=3,
            impact_threshold=5,
        )
        assert result == "PASS"
        assert code == 0

    def test_breaking_skip_impact_fails(self):
        """When impact is skipped, breaking drift always fails (conservative)."""
        result, code = gate_decision(
            breaking=True,
            skip_impact=True,
            impact_analyzed=False,
            total_downstream=0,
            impact_threshold=1,
        )
        assert result == "FAIL"
        assert code == 1

    def test_no_drift_passes(self):
        """Identical schemas produce no drift."""
        columns = _load_contract_columns()
        contract = _make_contract(columns)
        finding = detect_drift(contract, columns)

        assert finding.breaking is False
        assert len(finding.added) == 0
        assert len(finding.removed) == 0
        assert len(finding.type_changed) == 0

        result, code = gate_decision(
            breaking=False,
            skip_impact=True,
            impact_analyzed=False,
            total_downstream=0,
            impact_threshold=1,
        )
        assert result == "PASS"
        assert code == 0

    def test_breaking_impact_not_analyzed_fails(self):
        """If impact wasn't analyzed (e.g. no creds), breaking still fails."""
        result, code = gate_decision(
            breaking=True,
            skip_impact=False,
            impact_analyzed=False,
            total_downstream=0,
            impact_threshold=1,
        )
        assert result == "FAIL"
        assert code == 1


# ---------------------------------------------------------------------------
# Contract loading tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestContractLoading:
    """Contract and schema file loading."""

    def test_load_contract_json(self):
        contract, meta = load_contract(str(FACT_SALES_CONTRACT))
        assert contract.name == "fact_sales"
        assert len(contract.columns) == 9
        assert contract.columns[0].name == "sale_id"
        assert contract.columns[0].type == "INT64"
        assert contract.columns[0].nullable is False
        assert meta.get("lakehouse") == "Silver_Curated"

    def test_load_contract_yaml(self, tmp_path):
        yaml_content = """name: test_table
version: 1
owner: tester
columns:
  - name: id
    type: INT64
    nullable: false
  - name: value
    type: STRING
    nullable: true
"""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(yaml_content, encoding="utf-8")
        contract, meta = load_contract(str(yaml_file))
        assert contract.name == "test_table"
        assert len(contract.columns) == 2
        assert contract.columns[0].type == "INT64"

    def test_load_proposed_schema(self):
        proposed = load_proposed_schema(str(DAY2_ADDITIVE))
        assert len(proposed) == 11
        names = [c["name"] for c in proposed]
        assert "channel_code" in names
        assert "loyalty_points" in names

    def test_auto_discover_contract(self):
        path = auto_discover_contract("fact_sales")
        assert Path(path).name == "fact_sales.json"

    def test_auto_discover_contract_missing(self):
        with pytest.raises(FileNotFoundError, match="nonexistent"):
            auto_discover_contract("nonexistent")


# ---------------------------------------------------------------------------
# Risk classification tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestRiskClassification:
    """Risk level matrix: breaking * downstream count."""

    @pytest.mark.parametrize(
        "breaking, downstream, expected",
        [
            (True, 25, "critical"),
            (True, 20, "critical"),
            (True, 10, "high_risk"),
            (True, 3, "medium_risk"),
            (True, 0, "medium_risk"),
            (False, 20, "high_risk"),
            (False, 8, "medium_risk"),
            (False, 3, "low_risk"),
            (False, 0, "safe"),
        ],
    )
    def test_risk_matrix(self, breaking, downstream, expected):
        assert risk_from_drift_and_impact(
            breaking=breaking, total_downstream=downstream,
        ) == expected


# ---------------------------------------------------------------------------
# Required changes tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestRequiredChanges:

    def test_breaking_changes(self):
        finding_dict = {
            "added": [{"name": "new_col"}],
            "removed": [{"name": "old_col"}],
            "type_changed": [{"name": "id", "from": "INT64", "to": "STRING"}],
            "nullability_changed": [],
        }
        changes = required_changes(finding_dict)
        assert len(changes) == 3
        assert any("new_col" in c for c in changes)
        assert any("old_col" in c for c in changes)
        assert any("INT64" in c and "STRING" in c for c in changes)

    def test_additive_only(self):
        finding_dict = {
            "added": [{"name": "channel_code"}, {"name": "loyalty_points"}],
            "removed": [],
            "type_changed": [],
            "nullability_changed": [],
        }
        changes = required_changes(finding_dict)
        assert len(changes) == 1
        assert "channel_code" in changes[0]

    def test_no_changes(self):
        finding_dict = {
            "added": [],
            "removed": [],
            "type_changed": [],
            "nullability_changed": [],
        }
        changes = required_changes(finding_dict)
        assert changes == ["No schema changes detected."]


# ---------------------------------------------------------------------------
# Drift detection integration tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestDriftDetection:
    """End-to-end drift detection with real schema files."""

    def test_additive_drift(self):
        contract, _ = load_contract(str(FACT_SALES_CONTRACT))
        proposed = load_proposed_schema(str(DAY2_ADDITIVE))
        finding = detect_drift(contract, proposed)

        assert finding.breaking is False
        assert len(finding.added) == 2
        assert {c["name"] for c in finding.added} == {"channel_code", "loyalty_points"}
        assert len(finding.removed) == 0
        assert len(finding.type_changed) == 0
        assert len(finding.nullability_changed) == 0

    def test_breaking_drift(self):
        contract, _ = load_contract(str(FACT_SALES_CONTRACT))
        proposed = load_proposed_schema(str(DAY2_BREAKING))
        finding = detect_drift(contract, proposed)

        assert finding.breaking is True
        assert len(finding.removed) == 1
        assert finding.removed[0]["name"] == "discount_pct"
        assert len(finding.type_changed) == 2
        changed_names = {c["name"] for c in finding.type_changed}
        assert changed_names == {"sale_id", "sale_date"}
        assert len(finding.added) == 1
        assert finding.added[0]["name"] == "channel_code"
