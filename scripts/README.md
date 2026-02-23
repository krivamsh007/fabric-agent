# 📁 Scripts Folder - Complete Documentation

## Overview

This folder contains all CLI scripts for the Fabric Agent project. This document provides:
1. **Keep/Remove Analysis** - Which scripts to keep vs. delete
2. **Usage Guide** - How to use each essential script
3. **Testing Guide** - How to test the scripts
4. **Architecture Flow** - How scripts interact

---

# 🗂️ Script Inventory & Classification

## ✅ ESSENTIAL SCRIPTS (Keep)

| Script | Lines | Purpose | Status |
|--------|-------|---------|--------|
| `bootstrap_enterprise_domains.py` | 2,477 | **MAIN** bootstrap - creates full enterprise architecture | ✅ Core |
| `validate_enterprise_environment.py` | 324 | Validates all assets exist correctly | ✅ Core |
| `update_enterprise_notebooks.py` | 415 | Fixes/updates notebooks if issues | ✅ Support |
| `create_shortcuts.py` | 284 | Creates shortcuts after tables exist | ✅ Core |
| `seed_enterprise_data.py` | 275 | Uploads CSVs to OneLake | ✅ Core |
| `list_capacities.py` | 12 | Lists available Fabric capacities | ✅ Utility |
| `build_workspace_graph.py` | 101 | Builds dependency graph JSON | ✅ Utility |
| `safe_measure_rename.py` | 556 | End-to-end measure rename with rollback | ✅ Core Feature |
| `test_all_components.py` | 967 | Comprehensive test suite | ✅ Testing |

## ⚠️ OPTIONAL SCRIPTS (Keep but mark as optional)

| Script | Lines | Purpose | Status |
|--------|-------|---------|--------|
| `test_e2e_complete.py` | 801 | Full E2E testing | ⚠️ Optional |
| `test_impact_analysis.py` | 198 | Impact analysis demos | ⚠️ Optional |
| `test_standalone.py` | 405 | Standalone component tests | ⚠️ Optional |
| `validate_code.py` | 235 | Syntax validation | ⚠️ Optional |
| `cleanup_unidentified_tables.py` | 179 | Fix unidentified table issues | ⚠️ Troubleshooting |

## ❌ REMOVE SCRIPTS (Duplicates/Obsolete)

| Script | Lines | Reason to Remove |
|--------|-------|------------------|
| `bootstrap_unified.py` | 1,035 | **Duplicate** - superseded by `bootstrap_enterprise_domains.py` |
| `bootstrap_complete.py` | 445 | **BROKEN** (syntax error line 322) + duplicate |
| `bootstrap_refactor_enterprise_complex.py` | 1,141 | **Obsolete** - single-workspace version, replaced by multi-domain |
| `01_setup_data.py` | 13 | **Alias only** - just calls `seed_enterprise_data.py` |
| `clone_semantic_model.py` | 44 | **Debug utility** - not part of main workflow |
| `debug_resolve_enterprise_ids.py` | 87 | **Debug utility** - development only |
| `patch_pyspark_invalid_escape.py` | 51 | **Workaround** - should be in troubleshooting docs |

---

# 🚀 ESSENTIAL SCRIPTS - Detailed Guide

## 1️⃣ bootstrap_enterprise_domains.py (MAIN BOOTSTRAP)

### Purpose
Creates the complete enterprise Fabric architecture:
- 3 Domains (DataPlatform, SalesAnalytics, Finance)
- 5 Workspaces (DEV/PROD environments)
- 6 Lakehouses (Bronze, Silver, Gold, Analytics, Finance)
- 6 Notebooks (ETL pipeline)
- 2 Pipelines (Daily ETL, Hourly Incremental)
- 2 Semantic Models (98 measures)
- Shortcuts for cross-workspace data access

### Usage

```bash
# Step 1: Discover your capacity ID
python scripts/list_capacities.py

# Step 2: Bootstrap with separate workspaces per domain
python scripts/bootstrap_enterprise_domains.py \
    --prefix ENT \
    --capacity-id "your-capacity-guid" \
    --yes

# OR: Use a single existing workspace
python scripts/bootstrap_enterprise_domains.py \
    --workspace "my-existing-workspace" \
    --capacity-id "your-capacity-guid" \
    --yes

# Dry run (preview only)
python scripts/bootstrap_enterprise_domains.py \
    --prefix ENT \
    --dry-run

# Bootstrap specific domain only
python scripts/bootstrap_enterprise_domains.py \
    --domain DataPlatform \
    --capacity-id "your-capacity-guid" \
    --yes
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--prefix` | No | Prefix for all items (default: `ENT`) |
| `--workspace` | No | Use single existing workspace instead of creating multiple |
| `--capacity-id` | Recommended | Fabric capacity GUID for workspace assignment |
| `--domain` | No | Bootstrap specific domain only |
| `--enable-reports` | No | Create placeholder reports |
| `--skip-shortcuts` | No | Skip shortcuts (create later after seeding) |
| `--dry-run` | No | Preview only, no changes |
| `--yes` / `-y` | No | Skip confirmation prompt |
| `--output` | No | Output file for results JSON |

### What It Creates

```
DOMAIN: DataPlatform (Producer)
├── ENT_DataPlatform_DEV
│   ├── Bronze_Landing (Lakehouse)
│   ├── Silver_Curated (Lakehouse)
│   ├── Gold_Published (Lakehouse)
│   ├── 01_Ingest_Bronze (Notebook)
│   ├── 02_Transform_Silver (Notebook)
│   ├── 03_Build_Dimensions (Notebook)
│   ├── 04_Build_Facts (Notebook)
│   ├── 05_Aggregate_Gold (Notebook)
│   ├── 06_Data_Quality_Checks (Notebook)
│   ├── PL_Daily_ETL (Pipeline)
│   └── PL_Hourly_Incremental (Pipeline)
│
├── ENT_DataPlatform_PROD
│   └── (Same structure)

DOMAIN: SalesAnalytics (Consumer)
├── ENT_SalesAnalytics_DEV
│   ├── Analytics_Sandbox (Lakehouse)
│   ├── Shortcuts → Gold tables
│   ├── Enterprise_Sales_Model (98 measures)
│   └── Reports (if --enable-reports)

DOMAIN: Finance (Consumer)
├── ENT_Finance_DEV
│   ├── Finance_Layer (Lakehouse)
│   ├── Shortcuts → Gold tables
│   └── Finance_Model (Semantic Model)
```

### Testing

```bash
# 1. Syntax check
python -m py_compile scripts/bootstrap_enterprise_domains.py

# 2. Dry run
python scripts/bootstrap_enterprise_domains.py --dry-run

# 3. Validate results
python scripts/validate_enterprise_environment.py
```

---

## 2️⃣ seed_enterprise_data.py (DATA SEEDING)

### Purpose
Uploads seed CSV files to OneLake and optionally creates Delta tables.

### Usage

```bash
# Upload CSVs to OneLake Files/seed/
python scripts/seed_enterprise_data.py

# Skip upload (if already uploaded manually)
python scripts/seed_enterprise_data.py --skip-upload

# Specify workspace
python scripts/seed_enterprise_data.py --workspace "ENT_DataPlatform_DEV"
```

### Data Flow

```
data/seed_data/
├── bronze/
│   ├── raw_sales_transactions.csv
│   ├── raw_inventory_snapshots.csv
│   └── raw_customer_events.csv
├── silver/
│   ├── dim_date.csv
│   ├── dim_customer.csv
│   ├── dim_product.csv
│   ├── dim_store.csv
│   └── fact_sales.csv
└── gold/
    ├── agg_daily_sales.csv
    ├── agg_customer_360.csv
    └── agg_inventory_health.csv

    ↓ Uploads to ↓

OneLake/
├── Bronze_Landing/Files/seed/bronze/*.csv
├── Silver_Curated/Files/seed/silver/*.csv
└── Gold_Published/Files/seed/gold/*.csv
```

---

## 3️⃣ create_shortcuts.py (SHORTCUT CREATION)

### Purpose
Creates OneLake shortcuts from consumer workspaces to producer tables.
**Must run AFTER tables exist** (after seeding + running notebooks).

### Usage

```bash
# Create shortcuts
python scripts/create_shortcuts.py

# Dry run
python scripts/create_shortcuts.py --dry-run

# Specify prefix
python scripts/create_shortcuts.py --prefix ENT
```

### Shortcuts Created

| Source | Target | Shortcut Name |
|--------|--------|---------------|
| Silver_Curated/Tables/dim_date | Analytics_Sandbox | dim_date |
| Silver_Curated/Tables/dim_customer | Analytics_Sandbox | dim_customer |
| Silver_Curated/Tables/dim_product | Analytics_Sandbox | dim_product |
| Silver_Curated/Tables/dim_store | Analytics_Sandbox | dim_store |
| Silver_Curated/Tables/fact_sales | Analytics_Sandbox | fact_sales |
| Gold_Published/Tables/agg_daily_sales | Analytics_Sandbox | agg_daily_sales |
| Gold_Published/Tables/agg_customer_360 | Analytics_Sandbox | agg_customer_360 |

---

## 4️⃣ validate_enterprise_environment.py (VALIDATION)

### Purpose
Validates the enterprise environment matches expected architecture.

### Usage

```bash
# Validate all
python scripts/validate_enterprise_environment.py

# Specify prefix
python scripts/validate_enterprise_environment.py --prefix ENT
```

### Checks Performed

| Check | Expected |
|-------|----------|
| Workspaces | 5 (DEV/PROD for each domain) |
| Lakehouses | 3 per DataPlatform, 1 per Consumer |
| Tables | Bronze: 3, Silver: 5, Gold: 3 |
| Notebooks | ≥6 in DataPlatform_DEV |
| Pipelines | ≥2 in DataPlatform_DEV |
| Semantic Models | 1 per consumer workspace |
| Measures | 98 in Enterprise_Sales_Model |
| Shortcuts | ≥1 per consumer workspace |

---

## 5️⃣ update_enterprise_notebooks.py (NOTEBOOK REPAIR)

### Purpose
Updates or repairs notebooks if they have issues (wrong lakehouse attachment, missing code, etc.)

### Usage

```bash
# Update all notebooks in DataPlatform workspaces
python scripts/update_enterprise_notebooks.py

# Specify workspace
python scripts/update_enterprise_notebooks.py --workspace "ENT_DataPlatform_DEV"

# Dry run
python scripts/update_enterprise_notebooks.py --dry-run
```

### When to Use

1. Notebooks don't have correct lakehouse attached
2. Notebook code was corrupted
3. Need to regenerate PySpark code for seeding

---

## 6️⃣ safe_measure_rename.py (REFACTORING)

### Purpose
Safely rename a DAX measure with:
- Impact analysis
- Checkpoint creation
- Reference updates
- Validation
- Rollback capability

### Usage

```bash
# Step 1: Impact analysis (dry run)
python scripts/safe_measure_rename.py \
    --workspace "ENT_SalesAnalytics_DEV" \
    --model "Enterprise_Sales_Model" \
    --old "Total Revenue" \
    --new "Gross Revenue" \
    --dry-run

# Step 2: Apply the rename
python scripts/safe_measure_rename.py \
    --workspace "ENT_SalesAnalytics_DEV" \
    --model "Enterprise_Sales_Model" \
    --old "Total Revenue" \
    --new "Gross Revenue" \
    --apply

# Step 3: Validate
python scripts/safe_measure_rename.py \
    --workspace "ENT_SalesAnalytics_DEV" \
    --model "Enterprise_Sales_Model" \
    --old "Total Revenue" \
    --validate

# Step 4: Rollback (if needed)
python scripts/safe_measure_rename.py \
    --rollback \
    --operation-id "<OP_ID from apply output>" \
    --workspace "ENT_SalesAnalytics_DEV"
```

---

## 7️⃣ build_workspace_graph.py (VISUALIZATION)

### Purpose
Builds a dependency graph JSON for workspace visualization.

### Usage

```bash
# Build graph
python scripts/build_workspace_graph.py \
    --workspace "ENT_SalesAnalytics_DEV" \
    --out memory/workspace_graph.json

# View in browser
python -m fabric_agent.ui.graph_viewer \
    --graph memory/workspace_graph.json \
    --open
```

---

## 8️⃣ test_all_components.py (TESTING)

### Purpose
Comprehensive test suite for all components.

### Usage

```bash
# Offline tests (no Fabric connection)
python scripts/test_all_components.py --offline --verbose

# Online tests (requires Fabric)
python scripts/test_all_components.py \
    --online \
    --workspace "ENT_SalesAnalytics_DEV"
```

---

# 🔄 Complete Workflow

## Initial Setup (One Time)

```bash
# 1. Configure environment
cp .env.template .env
# Edit .env with your Azure credentials

# 2. Discover capacity
python scripts/list_capacities.py
# Copy the capacity GUID

# 3. Bootstrap environment
python scripts/bootstrap_enterprise_domains.py \
    --prefix ENT \
    --capacity-id "your-capacity-guid" \
    --skip-shortcuts \
    --yes

# 4. Seed data
python scripts/seed_enterprise_data.py

# 5. Run notebooks in Fabric UI to create tables
# (Or use pipelines)

# 6. Create shortcuts (after tables exist)
python scripts/create_shortcuts.py

# 7. Validate
python scripts/validate_enterprise_environment.py
```

## Architecture Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           BOOTSTRAP FLOW                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. bootstrap_enterprise_domains.py                                     │
│     │                                                                   │
│     ├── Creates Workspaces (ENT_DataPlatform_DEV, etc.)                │
│     ├── Creates Lakehouses (Bronze, Silver, Gold)                      │
│     ├── Creates Notebooks (with lakehouse attachment)                   │
│     ├── Creates Pipelines (orchestration)                              │
│     ├── Creates Semantic Models (98 measures)                          │
│     └── [SKIP] Shortcuts (tables don't exist yet)                      │
│                                                                         │
│  2. seed_enterprise_data.py                                            │
│     │                                                                   │
│     └── Uploads CSVs to OneLake Files/seed/                            │
│                                                                         │
│  3. RUN NOTEBOOKS IN FABRIC UI                                          │
│     │                                                                   │
│     ├── 01_Ingest_Bronze → Creates Bronze tables                       │
│     ├── 02_Transform_Silver → Creates Silver tables                    │
│     └── 05_Aggregate_Gold → Creates Gold tables                        │
│                                                                         │
│  4. create_shortcuts.py                                                 │
│     │                                                                   │
│     └── Creates shortcuts (NOW works - tables exist)                   │
│                                                                         │
│  5. validate_enterprise_environment.py                                  │
│     │                                                                   │
│     └── Validates everything is correct                                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                          REFACTORING FLOW                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. build_workspace_graph.py                                            │
│     │                                                                   │
│     └── Builds dependency graph                                        │
│                                                                         │
│  2. safe_measure_rename.py --dry-run                                    │
│     │                                                                   │
│     └── Impact analysis (shows affected measures)                      │
│                                                                         │
│  3. safe_measure_rename.py --apply                                      │
│     │                                                                   │
│     ├── Creates checkpoint                                             │
│     ├── Updates measure name                                           │
│     └── Updates all references                                         │
│                                                                         │
│  4. safe_measure_rename.py --validate                                   │
│     │                                                                   │
│     └── Validates changes                                              │
│                                                                         │
│  5. safe_measure_rename.py --rollback (if needed)                       │
│     │                                                                   │
│     └── Restores from checkpoint                                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

# 🧹 Cleanup Actions

## Scripts to Delete

```bash
# Remove duplicate/obsolete scripts
rm scripts/bootstrap_unified.py          # Duplicate
rm scripts/bootstrap_complete.py         # Broken + Duplicate
rm scripts/bootstrap_refactor_enterprise_complex.py  # Obsolete
rm scripts/01_setup_data.py              # Just an alias
rm scripts/clone_semantic_model.py       # Debug utility
rm scripts/debug_resolve_enterprise_ids.py  # Debug utility
rm scripts/patch_pyspark_invalid_escape.py  # Should be in docs
```

## Root Directory Cleanup

```bash
# Remove duplicate at root
rm bootstrap_refactor_enterprise_complex.py  # Duplicate of scripts/ version
```

---

# 📊 Final Script Inventory

After cleanup, the scripts folder should contain:

```
scripts/
├── 🟢 bootstrap_enterprise_domains.py   # MAIN bootstrap
├── 🟢 seed_enterprise_data.py           # Data seeding
├── 🟢 create_shortcuts.py               # Shortcut creation
├── 🟢 validate_enterprise_environment.py # Validation
├── 🟢 update_enterprise_notebooks.py    # Notebook repair
├── 🟢 safe_measure_rename.py            # Refactoring
├── 🟢 build_workspace_graph.py          # Visualization
├── 🟢 list_capacities.py                # Utility
├── 🟡 test_all_components.py            # Testing
├── 🟡 test_e2e_complete.py              # E2E testing
├── 🟡 test_impact_analysis.py           # Impact demos
├── 🟡 test_standalone.py                # Standalone tests
├── 🟡 validate_code.py                  # Code validation
└── 🟡 cleanup_unidentified_tables.py    # Troubleshooting

Legend:
🟢 Essential - Always keep
🟡 Optional - Keep but mark as optional
```

**Total: 14 scripts (down from 21)**
**Lines saved: ~2,771 lines removed**
