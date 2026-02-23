from fabric_agent.schema.contracts import DataContract
from fabric_agent.schema.drift import detect_drift

def test_schema_drift_additive_only(tmp_path):
    contract_path = tmp_path / "c.yaml"
    contract_path.write_text("""name: t
version: 1
columns:
  - name: a
    type: string
    nullable: true
""", encoding="utf-8")
    c = DataContract.load(str(contract_path))
    observed = [{"name": "a", "type": "string", "nullable": True}, {"name": "b", "type": "int", "nullable": True}]
    finding = detect_drift(c, observed)
    assert len(finding.added) == 1
    assert finding.breaking is False

def test_schema_drift_breaking_removed(tmp_path):
    contract_path = tmp_path / "c.yaml"
    contract_path.write_text("""name: t
version: 1
columns:
  - name: a
    type: string
    nullable: true
  - name: b
    type: int
    nullable: true
""", encoding="utf-8")
    c = DataContract.load(str(contract_path))
    observed = [{"name": "a", "type": "string", "nullable": True}]
    finding = detect_drift(c, observed)
    assert finding.breaking is True
    assert len(finding.removed) == 1
