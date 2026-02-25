# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-02-24

### Added
- **Freshness Guard** -- detects SQL Endpoint sync lag via `refreshMetadata` LRO with configurable fnmatch SLA thresholds (live evidence: `raw_customer_events` was 6 days stale with zero Fabric notification)
- **Maintenance Guard** -- pre-validates table names before TableMaintenance API submission, preventing silent 4-9 min Spark failures on Trial capacity (control chars, schema-qualified names, registry lookup)
- **Guard Monitor** -- orchestrator composing both guards into `run_once()` / `run_loop()` with error isolation, email alerting, and audit trail recording
- `fabric_agent/guards/` subpackage with models, freshness_guard, maintenance_guard, and monitor modules
- `scan_freshness` MCP tool and `freshness-scan` CLI command
- `run_table_maintenance` MCP tool and `table-maintenance` CLI command
- `run_fabricops_freshness_scan.py` one-click enterprise freshness scanning script with workspace auto-discovery
- `run_fabricops_delta_maintenance.py` one-click enterprise Delta maintenance script with capacity-aware concurrency
- 8 Pydantic models for guard tool I/O (TableSyncInfo, FreshnessViolationInfo, ScanFreshnessInput/Output, TableValidationInfo, MaintenanceJobInfo, RunTableMaintenanceInput/Output)
- 32 unit tests in `tests/test_guards.py` (145 total across all test files)
- Python 3.13 classifier in pyproject.toml

## [1.0.3] - 2026-02-23

### Added
- SECURITY.md with vulnerability reporting policy and credential handling guidelines
- CODE_OF_CONDUCT.md (Contributor Covenant v2.1)
- CHANGELOG.md (this file)
- py.typed marker for PEP 561 compliance
- .pre-commit-config.yaml for automated code quality enforcement

### Fixed
- README clone URL now points to the correct GitHub repository

## [1.0.2] - 2026-02-22

### Added
- ActionType and Urgency enums in healing/models.py replacing magic strings
- FabricApiClient async context manager (__aenter__/__aexit__)
- Shortcut plan TTL (1 hour expiry) in FabricTools
- test_fabric_client_errors.py (401/403/429/503/CircuitBreaker -- 9 tests)
- test_e2e_healing.py (full pipeline broken to plan to approve to execute -- 8 tests)
- GitHub Actions CI workflow (ci.yml) with pytest + ruff + mypy
- @pytest.mark.unit markers on all unit tests

### Changed
- InMemoryVectorStore.delete() now returns bool
- OperationMemory.get_statistics() returns top-level aliases for cleaner API
- StateType import moved from inline to module level in healer.py

## [1.0.1] - 2026-02-20

### Added
- Enterprise blast radius analysis (get_enterprise_blast_radius MCP tool)
- blast-radius CLI command for multi-workspace impact analysis
- Lineage engine enhanced with shortcut nodes, report-to-model edges, model-to-table READS_FROM edges
- ImpactedAssetDetail, WorkspaceBlastRadius, EnterpriseBlastRadius dataclasses
- trigger_pipeline, refresh_semantic_model, validate_shortcut_schema_compatibility in healer
- Shortcut cascade management (ShortcutCascadeManager)
- 4 new MCP tools: scan_shortcut_cascade, build_shortcut_healing_plan, approve_shortcut_healing, execute_shortcut_healing
- shortcut-scan and shortcut-heal CLI commands
- 37 unit tests for shortcut cascade, 32 tests for enterprise cascade
- Bootstrap creates DataPlatform_Sales_Model, Sales_Report, and Finance workspace items

### Fixed
- BROKEN_SHORTCUT anomaly type set can_auto_heal=False (requires human review)
- NotImplementedError in orchestrator replaced with None return
- Empty-graph warning in detector (previously silent failure)
- Removed update_expression stub that raised NotImplementedError

## [1.0.0] - 2026-02-15

### Added
- Initial release
- Self-Healing Infrastructure: AnomalyDetector, SelfHealer, SelfHealingMonitor with scan-detect-fix loop
- Context Memory / Vector RAG: OperationMemory (ChromaDB + sentence-transformers), SessionContext
- Safe Refactoring: RefactorExecutor with full audit trail, rollback support, lineage-aware impact analysis
- MCP Server: 25+ tools for AI-native Fabric management (discovery, impact, refactor, healing, memory)
- CLI: fabric-agent with commands for all use cases
- Resilience: RetryPolicy (exponential backoff + jitter) and CircuitBreaker (Netflix Hystrix pattern)
- Lineage Engine: Multi-workspace dependency graph with measures, tables, notebooks, pipelines, reports
- Schema Drift Detection: Contract-based detection with additive vs breaking classification
- Enterprise Bootstrap: bootstrap_enterprise_domains.py for workspace hierarchy provisioning
- Fabric API Client: Async client with ClientSecretCredential, LRO polling, OneLake DFS upload
- GitHub Actions: PyPI publish workflow, Fabric impact analysis workflow, auto-heal workflow
- Documentation: README with FAANG parallels, PREREQUISITES, ENTERPRISE_ARCHITECTURE, proofs

[1.1.0]: https://github.com/krivamsh007/fabric-agent/compare/v1.0.3...v1.1.0
[1.0.3]: https://github.com/krivamsh007/fabric-agent/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/krivamsh007/fabric-agent/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/krivamsh007/fabric-agent/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/krivamsh007/fabric-agent/releases/tag/v1.0.0
