from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from fabric_agent.healing.detector import AnomalyDetector
from fabric_agent.healing.models import AnomalyType


def _old_refresh(hours: int = 48) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


@pytest.mark.asyncio
async def test_stale_table_includes_pipeline_from_graph_edges() -> None:
    detector = AnomalyDetector(stale_hours=24)
    graph = {
        "nodes": [
            {"id": "pipeline-1", "name": "PL_Daily_ETL", "type": "pipeline", "metadata": {}},
            {
                "id": "lh-1:table:fact_sales",
                "name": "fact_sales",
                "type": "table",
                "metadata": {"last_refresh": _old_refresh()},
            },
        ],
        "edges": [
            {
                "source": "pipeline-1",
                "target": "lh-1:table:fact_sales",
                "type": "ref_table",
            }
        ],
    }

    anomalies = await detector._check_stale_tables("ws-1", graph)

    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.anomaly_type == AnomalyType.STALE_TABLE
    assert anomaly.metadata["pipeline_id"] == "pipeline-1"
    assert anomaly.metadata["pipeline_name"] == "PL_Daily_ETL"


@pytest.mark.asyncio
async def test_stale_table_uses_metadata_pipeline_fallback() -> None:
    detector = AnomalyDetector(stale_hours=24)
    graph = {
        "nodes": [
            {
                "id": "lh-1:table:fact_sales",
                "name": "fact_sales",
                "type": "table",
                "metadata": {
                    "last_refresh": _old_refresh(),
                    "pipeline_id": "pipeline-2",
                    "pipeline_name": "PL_Hourly_Incremental",
                },
            }
        ],
        "edges": [],
    }

    anomalies = await detector._check_stale_tables("ws-1", graph)

    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.metadata["pipeline_id"] == "pipeline-2"
    assert anomaly.metadata["pipeline_name"] == "PL_Hourly_Incremental"


@pytest.mark.asyncio
async def test_schema_drift_anomaly_carries_observed_schema_and_yaml_contract(tmp_path) -> None:
    contract_dir = tmp_path / "contracts"
    contract_dir.mkdir(parents=True, exist_ok=True)
    (contract_dir / "fact_sales.yaml").write_text(
        "\n".join(
            [
                "name: fact_sales",
                "version: 1",
                "owner: data-platform",
                "columns:",
                "  - name: id",
                "    type: int",
                "    nullable: false",
            ]
        ),
        encoding="utf-8",
    )

    detector = AnomalyDetector(contracts_dir=str(contract_dir))
    observed_schema = [
        {"name": "id", "type": "int", "nullable": False},
        {"name": "new_col", "type": "string", "nullable": True},
    ]
    graph = {
        "nodes": [
            {
                "id": "lh-1:table:fact_sales",
                "name": "fact_sales",
                "type": "table",
                "metadata": {"schema": observed_schema},
            }
        ],
        "edges": [],
    }

    anomalies = await detector._check_schema_drift("ws-1", graph)

    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.anomaly_type == AnomalyType.SCHEMA_DRIFT
    assert anomaly.can_auto_heal is True
    assert anomaly.metadata["observed_schema"] == observed_schema
    assert anomaly.metadata["contract_path"].endswith("fact_sales.yaml")
