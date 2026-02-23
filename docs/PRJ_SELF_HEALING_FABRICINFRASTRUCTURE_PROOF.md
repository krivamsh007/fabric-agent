# PRJ_SelfHealing_FabricInfrastructure Proof Document

This document captures the proof pack for the notebook:
`PRJ_SelfHealing_FabricInfrastructure` in workspace `ENT_DataPlatform_DEV`.

## Scope

This proof validates the self-healing operations workflow:

1. Authenticated workspace health scan using Fabric REST APIs.
2. Anomaly detection across workspace assets.
3. Healing plan generation (auto vs manual actions).
4. Dry-run healing execution with safe policy gates.
5. Health report output for auditability.

Out of scope for this document:

1. Bronze ingestion schema evolution (`01_Ingest_Bronze`).
2. Cross-workspace downstream schema impact email flow.

## Environment Under Test

1. Workspace: `ENT_DataPlatform_DEV`
2. Notebook: `PRJ_SelfHealing_FabricInfrastructure`
3. Runtime: Fabric Notebook (PySpark)
4. Package import path validated: `fabric_agent.healing`

## Verified Result Snapshot

Example validated run returned:

1. `total_assets: 24`
2. `anomalies_found: 1`
3. `manual_required: 1`
4. `health_score: 0.9583`
5. `anomaly_type: orphan_asset`
6. `asset_name: MyDemo`
7. `healing_result.success: true`
8. `dry_run: true`

Interpretation:

1. The monitor scanned real workspace inventory (not mock mode).
2. It found a genuine governance issue (orphan semantic model).
3. It generated a manual-review action and skipped changes in dry-run mode.

## Captured Evidence (From Provided Runs)

Concrete validated output from `PRJ_SelfHealing_FabricInfrastructure`:

1. `workspace_ids: ['0359f4ba-9cd9-4652-8438-3b77368a3cb7']`
2. `total_assets: 24`
3. `healthy: 23`
4. `anomalies_found: 1`
5. `manual_required: 1`
6. `health_score: 0.9583333333333334`
7. Anomaly identified:
   - `anomaly_type: orphan_asset`
   - `asset_name: MyDemo`
   - `can_auto_heal: false`
8. Healing execution summary:
   - `dry_run: true`
   - `applied: 0`
   - `skipped: 1`
   - `success: true`

## Acceptance Criteria

The PRJ notebook is considered proven when all checks below pass:

1. `fabric_agent.healing` imports without module errors.
2. Health scan shows `total_assets > 0`.
3. Health scan returns a valid `scan_id` and `workspace_ids`.
4. At least one anomaly appears in `healing_plan.anomalies` when present in workspace.
5. `healing_result.success` is `true` in dry run.
6. `manual_required` correctly reflects anomalies that are not auto-healable.

## Repro Steps

1. Open `PRJ_SelfHealing_FabricInfrastructure` in `ENT_DataPlatform_DEV`.
2. Start a new notebook session.
3. Run bootstrap/install cells.
4. Run the authenticated scan cell (the notebook now uses `FabricApiClient` by default).
5. Print the report as JSON and verify the acceptance criteria above.

## Evidence Checklist (for GitHub)

Add screenshots to your repo (for example: `docs/assets/self_healing/`) and link them here.

1. `E1` Import proof:
   `import fabric_agent` and `from fabric_agent.healing import ...` success output.
2. `E2` Authenticated scan proof:
   output showing `total_assets > 0`.
3. `E3` Detection proof:
   output with `anomalies_found`, `anomaly_type`, `asset_name`.
4. `E4` Plan and execution proof:
   output with `manual_required`, `healing_plan`, `healing_result`.
5. `E5` Full JSON proof:
   screenshot of the printed `Health Report Summary`.

## Screenshot Index

Use these exact filenames under `docs/assets/self_healing/`:

1. `E1_imports_ok.png`  
   ![E1 imports OK](assets/self_healing/E1_imports_ok.png)
2. `E2_authenticated_scan_total_assets.png`  
   ![E2 authenticated scan total assets](assets/self_healing/E2_authenticated_scan_total_assets.png)
3. `E3_anomaly_detected_orphan_asset.png`  
   ![E3 anomaly detected orphan asset](assets/self_healing/E3_anomaly_detected_orphan_asset.png)
4. `E4_healing_plan_and_dry_run_result.png`  
   ![E4 healing plan and dry run result](assets/self_healing/E4_healing_plan_and_dry_run_result.png)
5. `E5_health_report_summary_json.png`  
   ![E5 health report summary json](assets/self_healing/E5_health_report_summary_json.png)

## Known Non-Blocking Warnings

Pydantic may print warnings such as protected namespace conflicts for fields prefixed with `model_`.
These warnings do not block scan, planning, or dry-run execution.

Optional notebook suppression:

```python
import warnings
warnings.filterwarnings(
    "ignore",
    message='Field ".*" .* protected namespace "model_".*'
)
```

## Claim Statement for Public README

`PRJ_SelfHealing_FabricInfrastructure` is validated as an AI-assisted Fabric ops monitor that can:

1. discover workspace health issues,
2. generate safe remediation plans,
3. execute policy-controlled dry-run healing,
4. and produce auditable structured reports.
