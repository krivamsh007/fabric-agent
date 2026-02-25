# Fabric Agent — AI-Powered Infrastructure Automation for Microsoft Fabric

A **production-grade Python package** that brings autonomous self-healing, semantic memory,
and safe refactoring to Microsoft Fabric data platforms — wired up to Claude (and any MCP-compatible AI) as intelligent tools.

> Built as a portfolio-quality engineering showcase and learning resource.
> Every module includes "FAANG Parallel" commentary that explains what major tech companies
> call this same pattern in their own infrastructure.

---

## What Problem Does This Solve?

Enterprise Fabric environments drift. Measures get renamed and downstream reports break silently.
Schema contracts change and nobody notices until a pipeline fails at 3 AM.
Shortcuts point to deleted lakehouses. There is no automated detection, no institutional memory,
and no audit trail for who changed what and why.

This project builds four things to address that:

1. **Self-Healing Infrastructure** — an autonomous scan-detect-fix loop that finds broken
   shortcuts, schema drift, stale tables, and orphaned assets, then auto-heals what it can
   and pages you for the rest.

2. **Context Memory / Vector RAG** — every operation (rename, refactor, rollback) is embedded
   into a vector database so the agent can say "we tried this rename 6 weeks ago and it failed
   for this reason" before you try it again.

3. **Schema Drift Detection & Impact Analysis** — YAML-based schema contracts detect breaking
   changes (dropped columns, type changes) before they cascade. A multi-workspace lineage engine
   answers "what breaks if I change this table?" with full blast radius analysis.

4. **Data Freshness & Maintenance Guards** — proactive monitoring that catches SQL Endpoint sync
   lag and validates table maintenance jobs before they waste Spark compute.

---

## FAANG Parallels — Why This Architecture Matters

These patterns are not new — FAANG-scale companies have been running them for years under
different names. This project implements the same ideas for Microsoft Fabric.

| This Project | FAANG Equivalent | Description |
|---|---|---|
| `SelfHealingMonitor` (scan → detect → heal loop) | Google SRE "Borgmon + Autopilot" | Google's cluster manager Borg constantly scans job health and restarts/reschedules failed workloads. This project does the same for Fabric lakehouses and shortcuts. |
| Anomaly detection before auto-heal | Netflix Chaos Engineering / Hystrix | Netflix's Chaos Monkey deliberately breaks things to test resilience; Hystrix detects circuit breaks and falls back automatically. Our `AnomalyDetector` finds breaks before they cascade. |
| `OperationMemory` (vector RAG over history) | GitHub Copilot context retrieval | Copilot embeds your codebase and retrieves semantically similar code. Our agent embeds operation history and retrieves "what happened last time we tried this." |
| `SessionContext` (cross-session state) | Meta's Scribe / Memento | Meta's internal tools persist agent context across sessions. Our `SessionContext` saves "last session we renamed 3 measures and rolled back Total Revenue." |
| `AuditTrail` (immutable log with rollback) | Airbnb's Minerva / LinkedIn's DataHub | Both systems maintain a full lineage + audit record of every schema change. Our `MemoryManager` does this at the operation level with SQLite-backed snapshots. |
| MCP tool interface | Internal FAANG "data platform CLI" | Google's `dremel`, Meta's `presto` CLI, Airbnb's `Airflow` UI — all expose data operations as structured API calls. MCP is the same pattern for AI-native tooling. |
| `LineageEngine` (multi-workspace dependency graph) | LinkedIn Atlas / Apache Atlas | LinkedIn Atlas maps table → report → dashboard lineage across 10,000+ datasets. Our `LineageEngine` does this for Fabric measures, notebooks, and lakehouses. |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        MCP Clients (Claude, etc.)                        │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │  Model Context Protocol (MCP)
┌─────────────────────────────────▼───────────────────────────────────────┐
│                          mcp_server.py                                   │
│  (routes tool calls → FabricTools methods → validated Pydantic I/O)     │
└──────┬──────────────┬──────────────┬──────────────┬──────────────────────┘
       │              │              │              │
  ┌────▼────┐   ┌─────▼─────┐  ┌────▼────┐   ┌────▼────────┐
  │Discovery│   │  Impact   │  │Refactor │   │   Healer    │
  │ Agent   │   │  Agent    │  │ Agent   │   │   Agent     │
  └────┬────┘   └─────┬─────┘  └────┬────┘   └────┬────────┘
       │              │              │              │
┌──────▼──────────────▼──────────────▼──────────────▼──────────────────────┐
│                          Core Services                                    │
│  FabricApiClient │ LineageEngine │ SchemaDrift │ RefactorExecutor         │
│  AnomalyDetector │ SelfHealer   │ SelfHealingMonitor                     │
└──────┬──────────────────────────────────────────────────────┬────────────┘
       │                                                      │
  ┌────▼────────────────────────────────────────────┐   ┌────▼──────────────┐
  │              Memory Layer                       │   │   Microsoft       │
  │  OperationMemory (ChromaDB vector store)        │   │   Fabric REST API │
  │  SessionContext  (cross-session state)          │   │   OneLake DFS API │
  │  AuditTrail      (SQLite snapshots + rollback)  │   └───────────────────┘
  └─────────────────────────────────────────────────┘
```

### Module Map

| Directory | Purpose |
|-----------|---------|
| `fabric_agent/agents/` | Specialized agents (Discovery, Impact, Refactor, Healer) |
| `fabric_agent/guards/` | Data freshness SLA enforcement + table maintenance validation (Use Case 4) |
| `fabric_agent/healing/` | Self-healing: models, detector, healer, monitor (Use Case 1) |
| `fabric_agent/memory/` | Vector RAG over operation history (Use Case 2) |
| `fabric_agent/lineage/` | Multi-workspace dependency graph engine |
| `fabric_agent/schema/` | Schema drift detection + contract enforcement |
| `fabric_agent/refactor/` | Safe DAX/measure refactoring with rollback |
| `fabric_agent/storage/` | SQLite audit trail + state snapshots |
| `fabric_agent/tools/` | Pydantic-validated MCP tool definitions |
| `fabric_agent/core/` | Fabric vs local environment detection |
| `scripts/` | Enterprise bootstrap, seed data, validation |
| `notebooks/` | Educational Fabric notebooks (deploy + run in Fabric) |
| `tests/` | Async test suite (no real Fabric API required) |

---

## Use Case 1: Self-Healing Fabric Infrastructure

### What it does

Every 30 minutes (configurable), the `SelfHealingMonitor` runs a full workspace scan:

1. **Detect** — `AnomalyDetector` checks for broken shortcuts, schema contract violations,
   orphaned assets, and stale tables using the lineage graph
2. **Plan** — splits anomalies into `auto_actions` (safe to fix immediately) and
   `manual_actions` (require human approval)
3. **Heal** — `SelfHealer` executes auto-fixable items: recreates shortcuts via the Fabric API,
   applies additive schema fixes, triggers stale upstream pipelines
4. **Persist** — writes a `HealthReport` to an OneLake Delta table (`fabric_agent_health_log`)
5. **Notify** — sends a Teams/Slack alert if any manual interventions are needed

### Run it locally

```bash
pip install -e ".[healing]"

# Scan and show what would be fixed (safe, read-only)
fabric-agent health-scan --workspace-id <your-workspace-id>

# Generate a healing plan (no changes applied yet)
fabric-agent heal --workspace-id <your-workspace-id> --dry-run

# Apply safe auto-fixes
fabric-agent heal --workspace-id <your-workspace-id> --apply
```

### Run it in Fabric (scheduled)

The bootstrap deploys `PRJ_SelfHealing_FabricInfrastructure` as a notebook in your
Fabric workspace. Wire it to the `PL_PRJ_SelfHealing_FabricInfrastructure` pipeline
for the 30-minute scheduled loop.

### FAANG Parallel — Google SRE Borgmon

Google's Borg cluster manager runs a continuous health-check loop on every job across
hundreds of thousands of machines. When a job goes unhealthy, Borg reschedules it
automatically before a human ever sees the problem.
Our `SelfHealingMonitor` is the same pattern scoped to a Fabric tenant:
constant polling, structured anomaly types, graduated response (auto-heal vs escalate).

---

## Use Case 2: Context Memory / Vector RAG

### What it does

Every operation — rename, refactor, rollback, schema fix — is automatically embedded
(converted to a 384-dimensional vector) and stored in ChromaDB.

When the agent proposes a new operation, it first queries this store:
*"Show me the 5 most similar past operations and how they turned out."*

The result is a `RiskContext` containing:
- Similar past operations with their outcomes (success / failed / rolled_back)
- Historical failure rate for this type of change
- Common failure reasons extracted from past error logs
- Recommendations based on what actually worked

No manual curation. No SQL queries. Semantic similarity finds relevant history
even when table names and measure names have changed.

### Run it locally

```bash
pip install -e ".[memory]"

# See what is currently indexed
fabric-agent memory stats

# Find past operations similar to a proposed change
fabric-agent memory search "rename Total Revenue to Gross Revenue"

# Rebuild the vector index from your SQLite audit log
fabric-agent memory reindex

# Show what the agent remembers from the last session
fabric-agent session summary
```

### FAANG Parallel — GitHub Copilot Codebase Retrieval

When you type a function in VS Code, Copilot doesn't search your whole codebase with
`grep`. It embeds your partial function into a vector and retrieves the most semantically
similar chunks from an indexed version of your repo.
Our `OperationMemory` does the same for data infrastructure operations:
the proposed change description is embedded and the most similar past operations
(by meaning, not by string matching) are retrieved as context for the agent.

---

## Use Case 3: Schema Drift Detection & Downstream Impact Analysis

### What it does

When a vendor changes a column type, drops a field, or renames a table, every downstream
asset — shortcuts, semantic models, reports, pipelines — breaks silently. Fabric has no
built-in schema contract system and no way to answer "what breaks if this table changes?"

This project solves both problems:

**Schema Drift Detection** — JSON-based `DataContract` schemas define expected columns,
types, and nullability for each table. When new data arrives, `detect_drift()` compares
the observed schema against the contract and classifies changes:
- **Additive** (safe): new columns added — no downstream breakage
- **Breaking**: columns removed, types changed, nullability flipped — downstream assets will fail

**Downstream Impact Analysis** — the `LineageEngine` builds a full dependency graph across
workspaces (lakehouses → tables → shortcuts → semantic models → measures → reports → pipelines).
When a change is proposed, `get_enterprise_blast_radius` traces every downstream asset that
would be affected, with risk scoring (LOW / MEDIUM / HIGH / CRITICAL).

**Shortcut Cascade Healing** — when a source lakehouse is deleted or renamed, every shortcut
pointing to it breaks, cascading failures to all downstream consumers. The `ShortcutCascadeManager`
discovers broken shortcuts via the Fabric API, traces the full cascade, and generates a
layer-by-layer healing plan (shortcut → table → model → pipeline → report).

### Run it in Fabric (automated schema guard)

The `01_Ingest_Bronze_AutoHeal` notebook (`notebooks/01_ingest_bronze_autoheal.py`) deploys
into your Fabric workspace and runs as a scheduled ingestion job with built-in schema protection.
Wire it to the `PL_Daily_ETL` pipeline for continuous automated drift detection.

**How the notebook works:**

1. **Ingests** source CSV from OneLake into a Bronze Delta table
2. **Loads or bootstraps** a JSON schema contract from `Files/contracts/{table}.schema.json`
3. **Detects drift** — compares incoming schema against the contract using `fabric_agent.schema.drift`
4. **Auto-heals additive changes** — writes the table with `mergeSchema`, bumps the contract
   version, writes an alert JSON, sends an email notification
5. **Blocks on breaking changes** — if columns were removed or types changed, writes a detailed
   alert, triggers live impact analysis via `WorkspaceGraphBuilder` + `GraphImpactAnalyzer`,
   sends an HTML email listing every impacted shortcut/model/report/pipeline, and optionally
   raises `RuntimeError` to fail the pipeline (`BREAK_ON_BREAKING_DRIFT=1`)

```
Day-1: vendor sends CSV with columns [A, B, C]
  → Contract created: {columns: [A, B, C]}
  → Table written. No drift.

Day-2: vendor adds column D
  → Drift: added=[D], breaking=False
  → Auto-heal: table written with mergeSchema, contract bumped to v2
  → Email: "Non-breaking drift auto-healed — 1 column added"

Day-3: vendor drops column B, changes type of C
  → Drift: removed=[B], type_changed=[C], breaking=True
  → Pipeline BLOCKED. No table write.
  → Impact analysis: 200 downstream assets affected, risk=CRITICAL
  → Email: full blast radius (shortcuts, models, reports, pipelines)
```

### Run it locally (CLI)

```bash
# Analyze blast radius: what breaks if fact_sales changes?
fabric-agent blast-radius --workspace-ids <ws-id> --target-table fact_sales

# Scan for broken shortcuts across workspaces
fabric-agent shortcut-scan --workspace-id <ws-id>

# Generate and execute a healing plan for broken shortcuts
fabric-agent shortcut-heal --workspace-id <ws-id> --apply

# Run the vendor schema drift impact script
python scripts/vendor_schema_drift_impact.py \
  --workspace ENT_DataPlatform_DEV \
  --lakehouse Silver_Curated \
  --table fact_sales
```

### MCP tools (for AI-native usage)

| Tool | Description |
|------|-------------|
| `scan_shortcut_cascade` | Find all broken shortcuts across workspaces |
| `build_shortcut_healing_plan` | Generate layer-by-layer auto + manual fix plan |
| `approve_shortcut_healing` | Human approval gate for manual actions |
| `execute_shortcut_healing` | Apply shortcut fixes (dry_run supported) |
| `get_enterprise_blast_radius` | "What breaks if I change X?" — multi-workspace |

### Live test results

From a live audit of `ENT_DataPlatform_DEV`:
- Lineage graph: **406 nodes / 942 edges** (294 measures, 14 external sources, 13 lakehouses,
  tables, shortcuts, notebooks, pipelines, reports)
- Blast radius for `fact_sales`: **200 impacted assets**, risk = **CRITICAL**
- 9 shortcuts discovered, all healthy at time of audit

### CI/CD Integration — Schema Gate

The `run_fabricops_schema_gate.py` script is designed as a **deployment gate** for Azure DevOps
or GitHub Actions. It blocks breaking schema changes before they reach UAT/PROD.

```bash
# PASS — additive changes (new columns, no type changes)
python scripts/run_fabricops_schema_gate.py \
  --proposed-schema examples/day2_additive.json \
  --contract schemas/fact_sales.json \
  --skip-impact
# exit 0 — deployment proceeds

# FAIL — breaking changes (removed columns, type changes)
python scripts/run_fabricops_schema_gate.py \
  --proposed-schema examples/day2_breaking.json \
  --contract schemas/fact_sales.json \
  --skip-impact
# exit 1 — deployment blocked

# Full gate with live blast radius (against Fabric workspace)
python scripts/run_fabricops_schema_gate.py \
  --proposed-schema examples/day2_breaking.json \
  --table fact_sales \
  --workspace-name-prefix ENT_ --all-workspaces
# exit 1 — 200 downstream assets impacted, risk=CRITICAL
```

The included `.github/workflows/schema-gate.yml` workflow runs automatically on PRs that
change `schemas/*.json` — it detects drift, computes blast radius, posts a PR comment with
pass/fail per table, and blocks the merge if any gate fails.

### FAANG Parallel — LinkedIn Atlas / Apache Atlas

LinkedIn Atlas maps table → report → dashboard lineage across 10,000+ datasets. When a schema
change is proposed, Atlas shows every downstream consumer that would break before the change is
deployed. Apache Atlas (the open-source version) provides the same graph-based impact analysis.
Our `LineageEngine` + `detect_drift()` implements this pattern for Microsoft Fabric — contract-based
schema validation combined with multi-workspace dependency traversal.

---

## Use Case 4: Data Freshness & Table Maintenance Guards

### What it does

Two proactive guards that catch problems Fabric doesn't alert you about:

**Freshness Guard** -- SQL Endpoint sync between Lakehouse (Delta) and SQL Analytics Endpoint
is asynchronous, opaque, and has no built-in alerting. Tables can silently fall days behind.
Live test evidence: `raw_customer_events` was **6 days stale** with zero Fabric notification.

The Freshness Guard triggers `refreshMetadata` on every SQL Endpoint, compares each table's
`lastSuccessfulSyncDateTime` against configurable SLA thresholds (fnmatch patterns), and
returns violations.

**Maintenance Guard** -- The Fabric TableMaintenance API accepts **any string** as a table name
(HTTP 202), then fails asynchronously wasting 4-9 minutes of Spark compute per bad submission.
Live test evidence: control characters, schema-qualified names, and nonexistent tables all
get accepted but fail silently.

The Maintenance Guard pre-validates every table name (control chars, schema-qualified format,
registry lookup) before submission, checks queue pressure, and submits sequentially for
Trial capacity compatibility.

### Run it locally

```bash
# Scan SQL Endpoint freshness across workspaces
fabric-agent freshness-scan -w <workspace-id>

# Validate table maintenance (dry run -- default, no jobs submitted)
fabric-agent table-maintenance -w <workspace-id>

# Submit maintenance jobs (live mode)
fabric-agent table-maintenance -w <workspace-id> --no-dry-run
```

### One-click enterprise scripts

```bash
# Freshness scan for all ENT_* workspaces
python scripts/run_fabricops_freshness_scan.py --all-workspaces --workspace-name-prefix ENT_

# Delta maintenance dry run for all ENT_* workspaces
python scripts/run_fabricops_delta_maintenance.py --all-workspaces --workspace-name-prefix ENT_

# Live maintenance run
python scripts/run_fabricops_delta_maintenance.py --all-workspaces --workspace-name-prefix ENT_ --live
```

Both scripts auto-discover workspaces, write JSON reports to `data/freshness_scans/` and
`data/maintenance_runs/`, and support `--fail-on-violation` / `--fail-on-job-failure` flags
for CI pipeline gating.

### FAANG Parallel -- Google BigQuery Monarch + Stripe API Gateway

Google tracks per-table replication lag in Monarch with independent SLA windows per table.
Stripe validates every API request against a JSON Schema contract at the edge -- rejecting
invalid requests at O(1) cost before burning backend compute. This project combines both
patterns for Fabric.

---

## Prerequisites

You need:

1. A **Microsoft Fabric workspace** assigned to **Trial or Fabric capacity**
   (free trial available at [app.fabric.microsoft.com](https://app.fabric.microsoft.com))
2. An **Azure AD (Entra) App Registration** with a client secret
3. The **service principal added to the workspace** as **Member** or **Admin**
4. Python 3.10+

Full step-by-step: [docs/PREREQUISITES.md](https://github.com/krivamsh007/fabric-agent/blob/main/docs/PREREQUISITES.md)

> Note: Being a "Fabric Admin" at the tenant level does **not** automatically grant
> workspace permissions. The service principal must be explicitly added as a workspace member.

---

## Installation

```bash
git clone https://github.com/krivamsh007/fabric-agent.git
cd fabric-agent

# Create and activate virtual environment
python -m venv .venv

# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# macOS / Linux
source .venv/bin/activate

pip install -U pip

# Core install (MCP tools, refactoring, lineage, audit trail)
pip install -e .

# With self-healing + memory (recommended for full demo)
pip install -e ".[healing,memory]"

# Everything including dev tools
pip install -e ".[all]"
```

---

## Configuration

```bash
# Windows
copy .env.template .env

# macOS / Linux
cp .env.template .env
```

Open `.env` and fill in your Azure credentials. At minimum you need:

```env
AZURE_TENANT_ID=<your-azure-tenant-id>
AZURE_CLIENT_ID=<your-azure-client-id>
AZURE_CLIENT_SECRET=<your-client-secret>
USE_INTERACTIVE_AUTH=false

FABRIC_WORKSPACE_NAME=fabric-refactor-demo
FABRIC_CAPACITY_ID=<your-capacity-guid>
```

> The Semantic Model is generated entirely from the Python definition in `scripts/bootstrap_enterprise_domains.py`
> (`generate_enterprise_sales_model_bim`). No template ID or pre-existing model is required.

To find your capacity GUID:

```bash
python scripts/list_capacities.py
```

---

## Quickstart: Bootstrap the Enterprise Domain Architecture

This repo ships with a full bootstrap that creates the enterprise workspace hierarchy used
in the demos:

- **ENT_DataPlatform_DEV** (producer) — Bronze / Silver / Gold lakehouses, ingestion notebooks
- **ENT_DataPlatform_PROD** (producer) — same structure for production
- **ENT_SalesAnalytics_DEV** (consumer) — shortcuts into Gold, Semantic Model, Reports
- **ENT_Finance_DEV** (consumer) — shortcuts into Gold, Semantic Model

### Step 1 — Bootstrap workspaces and items

```bash
python scripts/bootstrap_enterprise_domains.py --capacity-id "<CAPACITY_GUID>"
```

This is idempotent: re-running it skips items that already exist.

### Step 2 — Seed data

```bash
python scripts/seed_enterprise_data.py
```

Uploads synthetic CSV data to OneLake and creates Delta tables via the Fabric Load Table API.

### Step 3 — Validate

```bash
python scripts/validate_enterprise_environment.py
```

Checks workspaces, lakehouses, table row counts, and semantic model measure count.

### Step 4 — Deploy fabric_agent package to Fabric

```bash
python scripts/bootstrap_enterprise_domains.py \
  --deploy-package \
  --capacity-id "<CAPACITY_GUID>"
```

Builds the wheel, uploads it to `Bronze_Landing/Files/wheels/` via OneLake DFS,
and regenerates the tutorial notebooks referencing the new wheel version.

---

## Running the MCP Server

Wire Claude (or any MCP client) to this server to get natural-language control
over all tools:

```bash
# Start the MCP server
fabric-mcp

# Or add to Claude Desktop via mcp_config.json
```

All available MCP tools:

| Category | Tool | Description |
|----------|------|-------------|
| Discovery | `build_workspace_graph` | Build multi-workspace dependency graph |
| Discovery | `list_workspaces` | List all accessible workspaces |
| Impact | `analyze_change_impact` | What breaks if I change X? |
| Impact | `analyze_refactor_impact` | Full risk analysis with severity levels |
| Refactor | `safe_refactor` | Execute rename/move with rollback safety |
| Healing | `scan_workspace_health` | Detect all anomalies in a workspace |
| Healing | `build_healing_plan` | Plan auto + manual fixes |
| Healing | `execute_healing_plan` | Apply safe fixes (dry_run supported) |
| Shortcuts | `scan_shortcut_cascade` | Discover broken shortcuts + downstream cascade |
| Shortcuts | `build_shortcut_healing_plan` | Generate layer-by-layer fix plan |
| Shortcuts | `approve_shortcut_healing` | Human approval gate for manual actions |
| Shortcuts | `execute_shortcut_healing` | Apply shortcut fixes (dry_run supported) |
| Impact | `get_enterprise_blast_radius` | What breaks if table X changes? (multi-workspace) |
| Guards | `scan_freshness` | Detect SQL Endpoint sync lag across workspaces |
| Guards | `run_table_maintenance` | Validate + submit TableMaintenance jobs (dry_run supported) |
| Memory | `find_similar_operations` | Vector search over operation history |
| Memory | `get_risk_context` | Historical failure rate + recommendations |
| Memory | `reindex_operation_memory` | Rebuild vector index from audit log |
| Memory | `get_session_summary` | What did the agent do last session? |

---

## Testing

No real Fabric credentials needed for the test suite:

```bash
pip install -e ".[dev,memory,healing]"
pytest tests/ -v

# Key test files:
# tests/test_guards.py            -- freshness + maintenance guards (32 tests)
# tests/test_operation_memory.py  -- vector RAG with in-memory mocks
# tests/test_healing.py           -- self-healing with AsyncMock graph
```

---

## Project Documentation

| Document | Description |
|----------|-------------|
| [docs/PREREQUISITES.md](https://github.com/krivamsh007/fabric-agent/blob/main/docs/PREREQUISITES.md) | Full Azure + Fabric setup guide |
| [docs/ENTERPRISE_ARCHITECTURE.md](https://github.com/krivamsh007/fabric-agent/blob/main/docs/ENTERPRISE_ARCHITECTURE.md) | Domain architecture diagram + design decisions |
| [docs/PRJ_SELF_HEALING_FABRICINFRASTRUCTURE_PROOF.md](https://github.com/krivamsh007/fabric-agent/blob/main/docs/PRJ_SELF_HEALING_FABRICINFRASTRUCTURE_PROOF.md) | End-to-end proof: self-healing notebook run in Fabric |
| [docs/01_INGEST_BRONZE_PROOF.md](https://github.com/krivamsh007/fabric-agent/blob/main/docs/01_INGEST_BRONZE_PROOF.md) | End-to-end proof: schema-guarded ingestion in Fabric |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to set up a dev environment and contribute |

---

## Known Limitations

This project is a portfolio-quality reference implementation. The table below is an honest
feature support matrix — what works offline today vs. what requires a live Fabric session.

| Feature | Status | Notes |
|---|---|---|
| Self-Healing: anomaly **detection** (shortcuts, orphans, schema drift, stale tables, pipeline failures) | **Works locally** | Uses lineage graph + schema contracts; no Fabric API required offline |
| Self-Healing: anomaly **remediation** (stale tables, schema drift) | **Works locally** | `SelfHealer` dispatches to existing `drift.apply_plan()` and pipeline trigger stubs |
| Self-Healing: broken shortcut **recreation** | **Fabric session required** | Shortcut recreation uses the OneLake Shortcuts API; `source_path` metadata must be present in the lineage graph. Currently flagged for human review rather than auto-healed. |
| Context Memory / Vector RAG | **Works locally** | ChromaDB + `sentence-transformers/all-MiniLM-L6-v2` — no API key, no cloud service needed |
| Audit trail (`MemoryManager`) | **Works locally** | SQLite-backed, zero external dependencies |
| MCP tool interface | **Works locally** | All tools callable from Claude or any MCP-compatible client |
| Semantic model **refactoring** (`rename_measure`, `rollback`) | **Fabric session required** | `RefactorExecutor` depends on `semantic-link-sempy` (`pip install semantic-link-sempy`), which requires a Fabric Spark session or a live Power BI / Fabric workspace connection. Running locally without SemPy will raise `ImportError` at executor load time. Install the `[notebook]` extra for offline SemPy stubs. |
| Lineage graph (`LineageEngine`) | **Fabric session required** | Full graph traversal uses SemPy `FabricRestClient`. Offline: `AnomalyDetector` falls back to an empty graph (logged as a warning). |
| Data Freshness Guard (`scan_freshness`) | **Fabric session required** | Triggers `refreshMetadata` LRO on SQL Endpoints; requires live API access. SLA threshold configuration and validation logic work offline. |
| Table Maintenance Guard (`run_table_maintenance`) | **Fabric session required** | Submits TableMaintenance Spark jobs via Fabric API. Dry-run validation (control chars, schema-qualified, registry lookup) requires API access to list Delta tables. |
| Workspace bootstrap (`bootstrap_enterprise_domains.py`) | **Fabric session required** | Creates workspaces, lakehouses, notebooks, and pipelines via the Fabric REST API. Requires a service principal with Fabric Admin permissions and an F-SKU or Trial capacity. |

### SemPy Dependency Note

The `refactor` and `lineage` modules depend on `semantic-link-sempy`, which Microsoft ships as
part of the Fabric Spark runtime. It is not a standard PyPI package and **cannot be used outside
a Fabric session** without a Fabric license.

```bash
# Install with the notebook extra for Fabric-session usage:
pip install "fabric_agent[notebook]"

# Install with memory + healing extras for local-only usage (no SemPy):
pip install "fabric_agent[memory,healing]"
```

The memory, healing, and MCP routing modules have **no SemPy dependency** and work fully offline.

---

## Troubleshooting

**`403 FeatureNotAvailable` when creating a Lakehouse**
- Assign the workspace to a Trial or Fabric (F-SKU) capacity in the Fabric UI
  (Workspace settings → License info)
- Ensure the service principal is workspace **Member** or **Admin**
- Ensure your tenant allows service principals to use Fabric APIs
  (Fabric Admin portal → Tenant settings → Service principals)

**`ModuleNotFoundError: No module named 'fabric_agent.healing'` in a Fabric Notebook**
- The Fabric Spark session has an old version of the wheel cached.
- Fix: bump the version in `pyproject.toml`, rebuild, and redeploy with `--deploy-package`.
  `pip install` skips a wheel if the same version string is already installed.

**`409 Conflict` when creating a workspace**
- The workspace already exists. Re-run without `--create-workspace` (bootstrap is idempotent).

**MCP tools not visible in Claude**
- Check `mcp_config.json` points to the correct venv Python path.
- Run `fabric-mcp` directly and confirm it starts without errors.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup instructions and the contribution workflow.

Quick summary:
- New MCP tools: add Pydantic models to `tools/models.py`, implement in `tools/fabric_tools.py`, route in `mcp_server.py`
- New agents: extend `SimpleAgent` from `agents/base.py`
- All mutations must call `memory_manager.record_state(...)` for the audit trail
- Run `pytest tests/ -v` and confirm all tests pass before opening a PR

---

## License

MIT — see [LICENSE](LICENSE).
