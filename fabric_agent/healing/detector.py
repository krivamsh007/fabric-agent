"""
Anomaly Detector
================

Wraps existing detection systems (lineage engine, schema drift) to produce
a unified List[Anomaly] for the SelfHealer.

WHAT IT DOES:
    1. Builds the workspace lineage graph (LineageEngine)
    2. Finds broken shortcuts (shortcut nodes with no source edge)
    3. Finds orphaned assets (no downstream consumers)
    4. Checks schema drift for each table with a registered contract
    5. Detects stale tables (not refreshed within SLA)
    6. Detects pipeline failures (last_run_status != 'success')

DESIGN PATTERN:
    All detection is read-only — the detector never writes anything.
    This makes it safe to run continuously without side effects.

FAANG PARALLEL:
    Google SRE's Monarch alerting pipeline, Meta's ODS anomaly detection.
    The detector is the "observe" phase of the observe→decide→act loop.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from loguru import logger

from fabric_agent.healing.models import Anomaly, AnomalyType, RiskLevel

if TYPE_CHECKING:
    from fabric_agent.api.fabric_client import FabricApiClient


# Stale table SLA: tables not refreshed in this many hours are flagged
DEFAULT_STALE_HOURS = 24


class AnomalyDetector:
    """
    Scans Fabric workspaces for infrastructure anomalies.

    USAGE:
        detector = AnomalyDetector(client=fabric_client)
        anomalies = await detector.scan(workspace_ids=["ws-123"])
        broken = [a for a in anomalies if a.anomaly_type == AnomalyType.BROKEN_SHORTCUT]

    DEPENDENCY:
        Uses LineageEngine (lineage/engine.py) for graph traversal.
        Uses detect_drift (schema/drift.py) for contract checking.
        Direct Fabric API calls for pipeline/refresh status.
    """

    def __init__(
        self,
        client: Optional["FabricApiClient"] = None,
        contracts_dir: Optional[str] = None,
        stale_hours: int = DEFAULT_STALE_HOURS,
    ):
        """
        Args:
            client: Initialized FabricApiClient for API calls.
            contracts_dir: Path to directory containing DataContract JSON files.
                           Checked for each scanned table.
            stale_hours: Tables not refreshed within this window are flagged STALE.
        """
        self.client = client
        self.contracts_dir = Path(contracts_dir) if contracts_dir else None
        self.stale_hours = stale_hours

    def _resolve_contract_path(self, table_name: str) -> Optional[Path]:
        """
        Resolve a table contract file by trying common extensions.

        Supports YAML-first contracts while remaining compatible with JSON.
        """
        if not self.contracts_dir or not self.contracts_dir.exists():
            return None

        for ext in (".yaml", ".yml", ".json"):
            candidate = self.contracts_dir / f"{table_name}{ext}"
            if candidate.exists():
                return candidate
        return None

    @staticmethod
    def _build_table_pipeline_lookup(graph: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """
        Build a lookup of table_id -> pipeline metadata using graph edges.

        Preference order:
        1) direct pipeline -> table edge by node id
        2) name-based fallback when table IDs vary but names are stable
        """
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])

        node_index = {str(n.get("id", "")): n for n in nodes}
        table_name_to_pipeline: Dict[str, Dict[str, str]] = {}
        table_id_to_pipeline: Dict[str, Dict[str, str]] = {}

        for edge in edges:
            source_id = str(edge.get("source", ""))
            target_id = str(edge.get("target", ""))
            if not source_id or not target_id:
                continue

            source_node = node_index.get(source_id)
            target_node = node_index.get(target_id)
            if not source_node or not target_node:
                continue

            if source_node.get("type") != "pipeline":
                continue
            if target_node.get("type") not in ("table", "lakehouse_table"):
                continue

            pipeline_meta = {
                "pipeline_id": source_id,
                "pipeline_name": source_node.get("name", source_id),
            }
            table_id_to_pipeline[target_id] = pipeline_meta
            table_name = str(target_node.get("name", "")).lower()
            if table_name and table_name not in table_name_to_pipeline:
                table_name_to_pipeline[table_name] = pipeline_meta

        return {
            "by_id": table_id_to_pipeline,
            "by_name": table_name_to_pipeline,
        }

    async def scan(self, workspace_ids: List[str]) -> List[Anomaly]:
        """
        Full workspace scan — returns all detected anomalies.

        Runs all sub-checks and merges the results. Each check is
        independent — a failure in one does not abort the others.

        Args:
            workspace_ids: List of Fabric workspace IDs to scan.

        Returns:
            List of Anomaly objects sorted by severity (critical first).
        """
        anomalies: List[Anomaly] = []

        for workspace_id in workspace_ids:
            logger.info(f"Scanning workspace: {workspace_id}")

            try:
                graph = await self._build_graph(workspace_id)
            except Exception as e:
                logger.error(f"Failed to build graph for {workspace_id}: {e}")
                continue

            # Run all checks — collect, never abort
            checks = [
                self._check_broken_shortcuts(workspace_id, graph),
                self._check_orphan_assets(workspace_id, graph),
                self._check_schema_drift(workspace_id, graph),
                self._check_stale_tables(workspace_id, graph),
                self._check_pipeline_failures(workspace_id),
            ]

            for check in checks:
                try:
                    found = await check
                    anomalies.extend(found)
                except Exception as e:
                    logger.warning(f"Check failed in {workspace_id}: {e}")

        # Sort: critical → high → medium → low
        _order = {
            RiskLevel.CRITICAL: 0,
            RiskLevel.HIGH: 1,
            RiskLevel.MEDIUM: 2,
            RiskLevel.LOW: 3,
        }
        anomalies.sort(key=lambda a: _order.get(a.severity, 99))

        logger.info(f"Scan complete: {len(anomalies)} anomalies across {len(workspace_ids)} workspace(s)")
        return anomalies

    # ------------------------------------------------------------------ #
    # Internal helpers                                                      #
    # ------------------------------------------------------------------ #

    async def _build_graph(self, workspace_id: str) -> Dict[str, Any]:
        """
        Build the lineage graph for a workspace.

        Uses LineageEngine if available; falls back to direct API call
        for listing items (no SemPy required).

        Returns:
            Graph dict with "nodes" and "edges" keys.
        """
        if self.client is not None:
            try:
                from fabric_agent.tools.workspace_graph import WorkspaceGraphBuilder
                builder = WorkspaceGraphBuilder(include_measure_graph=False)
                return await builder.build(
                    client=self.client,
                    workspace_id=workspace_id,
                    workspace_name=workspace_id,
                )
            except Exception as e:
                logger.debug(f"WorkspaceGraphBuilder unavailable: {e}")

        # Minimal mock graph for unit tests / offline operation
        logger.warning(
            "WorkspaceGraphBuilder unavailable — scan returned empty graph. "
            "Install fabric_agent with lineage dependencies: pip install 'fabric_agent[healing]'"
        )
        return {"nodes": [], "edges": []}

    async def _check_broken_shortcuts(
        self, workspace_id: str, graph: Dict[str, Any]
    ) -> List[Anomaly]:
        """
        Detect shortcuts that reference non-existent source items.

        PHASE E UPGRADE — API-based detection:
            Old approach: graph-only — finds "shortcut node with no source edge"
                          but loses source_path → can_auto_heal=False always
            New approach: query Fabric REST API for actual shortcut definitions
                          (includes source_workspace_id, source_lakehouse_id, source_path)
                          then verify each source is accessible → enables auto-heal

        ALGORITHM:
            1. List all lakehouses in the workspace (GET /items?type=Lakehouse)
            2. For each lakehouse, list its shortcuts (GET /items/{lh_id}/shortcuts)
            3. Parse each shortcut → source workspace + lakehouse + path
            4. Probe each source (GET /workspaces/{src_ws}/items/{src_lh})
               200 = healthy, 4xx/exception = broken
            5. Broken shortcuts → Anomaly with can_auto_heal=True if source_path known

        FALLBACK:
            If client is None (offline / unit tests), falls back to graph-based
            detection. Graph-based sets can_auto_heal=False and heal_action="flag_for_review".

        RISK: HIGH — downstream items that depend on this shortcut will fail.
        AUTO-HEAL: YES when source_path is known (API-based path).
                   NO when using graph-only fallback (source_path unknown).

        FAANG PARALLEL:
            Netflix's Mantis streaming: when a source stream disappears, the
            system queries the stream registry (not just the runtime graph) to
            get the full definition, enabling automatic reconnection.
        """
        # --- API-based detection (preferred path when client is available) ---
        if self.client is not None:
            return await self._check_broken_shortcuts_api(workspace_id)

        # --- Graph-only fallback (offline / unit tests) ---
        return self._check_broken_shortcuts_graph(workspace_id, graph)

    async def _check_broken_shortcuts_api(
        self, workspace_id: str
    ) -> List[Anomaly]:
        """
        API-based broken shortcut detection.

        Queries the Fabric REST API for the full shortcut definition (including
        source paths), then verifies each source is accessible.

        This is the production path — it populates source_path in the anomaly
        metadata, enabling auto-heal via ShortcutCascadeManager.
        """
        anomalies: List[Anomaly] = []

        lakehouses = await self._list_lakehouses(workspace_id)
        for lh in lakehouses:
            lh_id = lh.get("id", "")
            lh_name = lh.get("displayName", "")
            if not lh_id:
                continue

            try:
                shortcuts = await self._list_shortcuts(workspace_id, lh_id)
            except Exception as exc:
                logger.warning(
                    f"AnomalyDetector: failed to list shortcuts for {lh_name}: {exc}"
                )
                continue

            for raw in shortcuts:
                name = raw.get("name", "")
                target = raw.get("target", {})
                one_lake = target.get("oneLake", {})

                if not one_lake:
                    # External shortcut (ADLS/S3/GCS) — skip for now
                    continue

                src_ws_id = one_lake.get("workspaceId", "")
                src_lh_id = one_lake.get("itemId", "")
                src_path = one_lake.get("path", f"Tables/{name}")

                if not src_ws_id or not src_lh_id:
                    # Incomplete shortcut definition — flag for review
                    anomalies.append(Anomaly(
                        anomaly_type=AnomalyType.BROKEN_SHORTCUT,
                        severity=RiskLevel.HIGH,
                        asset_id=f"{lh_id}:{name}",
                        asset_name=name,
                        workspace_id=workspace_id,
                        details=(
                            f"Shortcut '{name}' in lakehouse '{lh_name}' has an "
                            "incomplete target definition (missing workspaceId or itemId). "
                            "Manual review required."
                        ),
                        can_auto_heal=False,
                        heal_action="flag_for_review",
                        metadata={
                            "lakehouse_id": lh_id,
                            "lakehouse_name": lh_name,
                            "source_path": src_path,
                        },
                    ))
                    continue

                # Probe whether the source is accessible
                accessible = await self._source_accessible(src_ws_id, src_lh_id)
                if not accessible:
                    anomalies.append(Anomaly(
                        anomaly_type=AnomalyType.BROKEN_SHORTCUT,
                        severity=RiskLevel.HIGH,
                        asset_id=f"{lh_id}:{name}",
                        asset_name=name,
                        workspace_id=workspace_id,
                        details=(
                            f"Shortcut '{name}' in lakehouse '{lh_name}' points to "
                            f"workspace {src_ws_id} / lakehouse {src_lh_id} "
                            f"which is no longer accessible. "
                            f"Source path: {src_path}"
                        ),
                        can_auto_heal=True,   # We have the full source path — can recreate
                        heal_action="recreate_shortcut",
                        metadata={
                            "lakehouse_id": lh_id,
                            "lakehouse_name": lh_name,
                            "source_workspace_id": src_ws_id,
                            "source_lakehouse_id": src_lh_id,
                            "source_path": src_path,
                        },
                    ))

        logger.debug(f"Broken shortcuts (API): {len(anomalies)}")
        return anomalies

    def _check_broken_shortcuts_graph(
        self, workspace_id: str, graph: Dict[str, Any]
    ) -> List[Anomaly]:
        """
        Graph-only fallback for broken shortcut detection (offline / unit tests).

        Finds shortcut nodes in the lineage graph that have no incoming source
        edges. Source path is unknown in this path → can_auto_heal=False.
        """
        anomalies: List[Anomaly] = []
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])

        shortcut_nodes = [n for n in nodes if n.get("type") == "shortcut"]
        nodes_with_incoming = {e.get("target") for e in edges}

        for node in shortcut_nodes:
            node_id = node.get("id", "")
            if node_id not in nodes_with_incoming:
                anomalies.append(Anomaly(
                    anomaly_type=AnomalyType.BROKEN_SHORTCUT,
                    severity=RiskLevel.HIGH,
                    asset_id=node_id,
                    asset_name=node.get("name", "Unknown Shortcut"),
                    workspace_id=workspace_id,
                    details=(
                        f"Shortcut '{node.get('name')}' has no source edge in the lineage graph. "
                        "The source item may have been deleted or moved. "
                        "Source path unknown (graph-only detection) — manual review required. "
                        "For auto-heal capability, enable API-based detection by wiring a FabricApiClient."
                    ),
                    can_auto_heal=False,   # Source path unknown without API query
                    heal_action="flag_for_review",
                    metadata={"node": node},
                ))

        logger.debug(f"Broken shortcuts (graph fallback): {len(anomalies)}")
        return anomalies

    async def _list_lakehouses(self, workspace_id: str) -> List[Dict]:
        """
        GET /v1/workspaces/{ws_id}/items?type=Lakehouse

        Returns list of lakehouse item dicts.
        """
        try:
            data = await self.client.get(
                f"/workspaces/{workspace_id}/items",
                params={"type": "Lakehouse"},
            )
            return data.get("value", []) if isinstance(data, dict) else []
        except Exception as exc:
            logger.warning(f"AnomalyDetector: _list_lakehouses({workspace_id}) failed: {exc}")
            return []

    async def _list_shortcuts(
        self, workspace_id: str, lakehouse_id: str
    ) -> List[Dict]:
        """
        GET /v1/workspaces/{ws_id}/items/{lh_id}/shortcuts

        Returns raw shortcut objects from the Fabric API.
        """
        try:
            data = await self.client.get(
                f"/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"
            )
            return data.get("value", []) if isinstance(data, dict) else []
        except Exception as exc:
            logger.warning(
                f"AnomalyDetector: _list_shortcuts({workspace_id}, {lakehouse_id}) failed: {exc}"
            )
            return []

    async def _source_accessible(
        self, src_ws_id: str, src_lh_id: str
    ) -> bool:
        """
        GET /v1/workspaces/{src_ws_id}/items/{src_lh_id}

        200 = source lakehouse accessible (shortcut healthy).
        Any error = source gone (shortcut broken).
        """
        try:
            await self.client.get(f"/workspaces/{src_ws_id}/items/{src_lh_id}")
            return True
        except Exception:
            return False

    async def _check_orphan_assets(
        self, workspace_id: str, graph: Dict[str, Any]
    ) -> List[Anomaly]:
        """
        Find assets with no downstream consumers.

        An orphan is a semantic model or table-type node that:
        - Has no outgoing edges (nothing depends on it)
        - Has been around long enough to suspect abandonment

        RISK: LOW — these are candidates for cleanup, not active breakage.
        AUTO-HEAL: NO — never auto-delete. Flag for human review only.
        """
        anomalies: List[Anomaly] = []
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])

        nodes_with_outgoing = {e.get("source") for e in edges}

        orphan_types = {"semantic_model", "table", "measure"}
        for node in nodes:
            node_id = node.get("id", "")
            node_type = node.get("type", "")
            if node_type in orphan_types and node_id not in nodes_with_outgoing:
                anomalies.append(Anomaly(
                    anomaly_type=AnomalyType.ORPHAN_ASSET,
                    severity=RiskLevel.LOW,
                    asset_id=node_id,
                    asset_name=node.get("name", "Unknown"),
                    workspace_id=workspace_id,
                    details=(
                        f"Asset '{node.get('name')}' (type={node_type}) has no downstream consumers. "
                        "Consider archiving or removing to reduce workspace noise."
                    ),
                    can_auto_heal=False,   # NEVER auto-delete
                    heal_action="flag_for_review",
                    metadata={"node_type": node_type},
                ))

        logger.debug(f"Orphan assets: {len(anomalies)}")
        return anomalies

    async def _check_schema_drift(
        self, workspace_id: str, graph: Dict[str, Any]
    ) -> List[Anomaly]:
        """
        Check registered table contracts for schema drift.

        For each table node that has a matching DataContract file in
        contracts_dir, calls detect_drift() to compare the registered
        schema against the observed schema (from graph metadata).

        RISK:
            - Additive changes (new columns): LOW, can auto-heal
            - Breaking changes (removed/type-changed): HIGH, manual review

        AUTO-HEAL: ADDITIVE ONLY — calls drift.apply_plan() for additive changes.
        """
        if not self.contracts_dir or not self.contracts_dir.exists():
            return []

        anomalies: List[Anomaly] = []
        nodes = graph.get("nodes", [])

        for node in nodes:
            if node.get("type") not in ("table", "lakehouse_table"):
                continue

            node_name = node.get("name", "")
            contract_path = self._resolve_contract_path(node_name)
            if not contract_path:
                continue

            observed_schema = node.get("metadata", {}).get("schema", [])
            if not observed_schema:
                continue

            try:
                from fabric_agent.schema.contracts import DataContract
                from fabric_agent.schema.drift import detect_drift

                contract = DataContract.load(str(contract_path))
                finding = detect_drift(contract, observed_schema)

                if finding.breaking or finding.added:
                    severity = RiskLevel.HIGH if finding.breaking else RiskLevel.LOW
                    anomalies.append(Anomaly(
                        anomaly_type=AnomalyType.SCHEMA_DRIFT,
                        severity=severity,
                        asset_id=node.get("id", ""),
                        asset_name=node_name,
                        workspace_id=workspace_id,
                        details=finding.summary,
                        can_auto_heal=not finding.breaking,  # additive only
                        heal_action="apply_additive_drift" if not finding.breaking else None,
                        metadata={
                            "contract_path": str(contract_path),
                            "breaking": finding.breaking,
                            "added_count": len(finding.added),
                            "removed_count": len(finding.removed),
                            "observed_schema": observed_schema,
                        },
                    ))
            except Exception as e:
                logger.warning(f"Schema drift check failed for {node_name}: {e}")

        logger.debug(f"Schema drift anomalies: {len(anomalies)}")
        return anomalies

    async def _check_stale_tables(
        self, workspace_id: str, graph: Dict[str, Any]
    ) -> List[Anomaly]:
        """
        Flag tables not refreshed within the SLA window (default: 24h).

        Uses the 'last_refresh' metadata from the graph node if available,
        or queries the Fabric Dataset Refresh API.

        RISK: MEDIUM — data may be out of date but infra is not broken.
        AUTO-HEAL: YES — trigger the upstream pipeline via Fabric REST API.
        """
        anomalies: List[Anomaly] = []
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=self.stale_hours)
        pipeline_lookup = self._build_table_pipeline_lookup(graph)

        nodes = graph.get("nodes", [])
        for node in nodes:
            if node.get("type") not in ("table", "semantic_model", "lakehouse_table"):
                continue

            last_refresh_str = node.get("metadata", {}).get("last_refresh")
            if not last_refresh_str:
                continue

            try:
                last_refresh = datetime.fromisoformat(last_refresh_str)
                # Make timezone-aware if naive
                if last_refresh.tzinfo is None:
                    last_refresh = last_refresh.replace(tzinfo=timezone.utc)
                if last_refresh < cutoff:
                    hours_stale = int((now - last_refresh).total_seconds() / 3600)
                    table_id = str(node.get("id", ""))
                    table_name = str(node.get("name", "")).lower()
                    node_meta = node.get("metadata", {}) if isinstance(node.get("metadata"), dict) else {}
                    metadata_pipeline = {
                        "pipeline_id": node_meta.get("pipeline_id"),
                        "pipeline_name": node_meta.get("pipeline_name"),
                    }
                    pipeline_meta = (
                        (metadata_pipeline if metadata_pipeline.get("pipeline_id") else None)
                        or pipeline_lookup["by_id"].get(table_id)
                        or pipeline_lookup["by_name"].get(table_name)
                        or {}
                    )
                    anomalies.append(Anomaly(
                        anomaly_type=AnomalyType.STALE_TABLE,
                        severity=RiskLevel.MEDIUM,
                        asset_id=node.get("id", ""),
                        asset_name=node.get("name", "Unknown"),
                        workspace_id=workspace_id,
                        details=(
                            f"Table '{node.get('name')}' was last refreshed {hours_stale}h ago "
                            f"(SLA: {self.stale_hours}h). Data may be stale."
                        ),
                        can_auto_heal=True,
                        heal_action="trigger_pipeline_refresh",
                        metadata={
                            "last_refresh": last_refresh_str,
                            "hours_stale": hours_stale,
                            "sla_hours": self.stale_hours,
                            "pipeline_id": pipeline_meta.get("pipeline_id"),
                            "pipeline_name": pipeline_meta.get("pipeline_name"),
                        },
                    ))
            except ValueError:
                continue

        logger.debug(f"Stale tables: {len(anomalies)}")
        return anomalies

    async def _check_pipeline_failures(
        self, workspace_id: str
    ) -> List[Anomaly]:
        """
        Check for pipelines whose last run ended in failure.

        Queries the Fabric Items API for pipelines, then checks run history.
        Falls back gracefully if the API is unavailable.

        RISK: HIGH — broken pipelines stop data flowing to downstream tables.
        AUTO-HEAL: NO — pipeline failures often need code-level intervention.
        """
        if self.client is None:
            return []

        anomalies: List[Anomaly] = []
        try:
            data = await self.client.get(f"/workspaces/{workspace_id}/items")
            items = data.get("value", []) if isinstance(data, dict) else []
            pipelines = [i for i in items if str(i.get("type", "")).lower() == "datapipeline"]

            for pipeline in pipelines:
                pipeline_id = pipeline.get("id", "")
                pipeline_name = pipeline.get("displayName") or pipeline.get("name", "Unknown")

                try:
                    # Get last run status
                    runs_data = await self.client.get(
                        f"/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances",
                        params={"jobType": "Pipeline", "$top": 1},
                    )
                    runs = runs_data.get("value", []) if isinstance(runs_data, dict) else []

                    if runs:
                        last_run = runs[0]
                        status = str(last_run.get("status", "")).lower()
                        if status in ("failed", "cancelled", "error"):
                            anomalies.append(Anomaly(
                                anomaly_type=AnomalyType.PIPELINE_FAILURE,
                                severity=RiskLevel.HIGH,
                                asset_id=pipeline_id,
                                asset_name=pipeline_name,
                                workspace_id=workspace_id,
                                details=(
                                    f"Pipeline '{pipeline_name}' last run status: {status}. "
                                    "Downstream tables may not be refreshed."
                                ),
                                can_auto_heal=False,
                                heal_action=None,
                                metadata={
                                    "last_run_status": status,
                                    "last_run_id": last_run.get("id"),
                                    "last_run_start": last_run.get("startTimeUtc"),
                                },
                            ))
                except Exception as e:
                    logger.debug(f"Could not get run history for pipeline {pipeline_name}: {e}")

        except Exception as e:
            logger.debug(f"Pipeline failure check skipped: {e}")

        logger.debug(f"Pipeline failures: {len(anomalies)}")
        return anomalies
