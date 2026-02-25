"""
Shortcut Cascade Manager
========================

Discovers, analyzes, and heals broken OneLake shortcuts across all workspaces,
including the full cascade of impacted downstream assets.

WHAT IS A ONELAKE SHORTCUT?
    A shortcut is a symbolic link inside a Fabric lakehouse that points to data
    stored in another lakehouse (possibly in a different workspace). It lets
    multiple workspaces share the same physical data without duplication.

    Example:
        ENT_DataPlatform_DEV/Bronze_Landing/Tables/DimDate
            → (shortcut pointing to) →
        ENT_DataPlatform_PROD/Bronze_Landing/Tables/DimDate

    When the source workspace or lakehouse is renamed/deleted, the shortcut
    breaks — and every asset built on top of it (tables, semantic models,
    pipelines, reports) fails silently.

PROBLEM THIS SOLVES:
    1. Old approach: graph-based detection loses source_path when shortcut breaks
       → can_auto_heal=False always → operators must fix manually
    2. Old healer: wrong API endpoint (POST /items instead of POST /items/{lh}/shortcuts)
       → even if healing was attempted, it would create the wrong resource

SOLUTION:
    1. Query Fabric REST API for actual shortcut definitions (includes source paths)
    2. Verify each source exists (HEAD /workspaces/{src_ws}/items/{src_lh})
    3. Traverse lineage graph downstream from each broken shortcut
    4. Generate per-layer fix suggestions (shortcut → table → model → pipeline → report)
    5. Split into auto_actions (safe) vs manual_actions (needs human approval)

FAANG PARALLEL:
    Meta's data lineage system (Deltastreamer + LineageGraph):
      When a source partition is deleted, it automatically finds ALL dependent
      pipelines/models and proposes fixes in priority order.

    Google Cloud Dataplex lineage-aware governance:
      Stores full source+target metadata at discovery time. When source moves,
      the system knows where it went and can reconstruct the lineage.

    Amazon's Route 53 automated remediation:
      Auto-remediates safe changes (shortcut recreation), escalates risky ones
      (cross-region redirects) to the on-call engineer via PagerDuty.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from fabric_agent.healing.models import (
    ActionType,
    CascadeImpact,
    EnterpriseBlastRadius,
    FixSuggestion,
    ImpactedAssetDetail,
    ShortcutDefinition,
    ShortcutHealingPlan,
    ShortcutHealingResult,
    Urgency,
    WorkspaceBlastRadius,
)

from loguru import logger

if TYPE_CHECKING:
    from fabric_agent.api.fabric_client import FabricApiClient
    from fabric_agent.lineage.engine import LineageGraph
    from fabric_agent.lineage.models import AssetType as LineageAssetType
    from fabric_agent.storage.memory_manager import MemoryManager


class ShortcutCascadeManager:
    """
    Discovers, analyzes, and heals broken OneLake shortcuts + their cascade.

    USAGE (typical flow):
        manager = ShortcutCascadeManager(client=fabric_client)

        # Step 1: Discover all shortcuts + check health
        plan = await manager.build_cascade_plan(workspace_ids=["ws-001", "ws-002"])

        # Step 2: Human reviews plan.auto_actions + plan.manual_actions
        if plan.approval_required:
            # Wait for human approval via approve_shortcut_healing() MCP tool
            plan.approved_by = "john.doe@contoso.com"

        # Step 3: Execute
        result = await manager.execute_plan(plan)

    DRY RUN:
        Always call execute_plan(plan, dry_run=True) first to preview changes.
        The result shows exactly what would be created/refreshed without
        touching anything in Fabric.
    """

    def __init__(
        self,
        client: Optional["FabricApiClient"] = None,
        memory: Optional["MemoryManager"] = None,
    ):
        """
        Args:
            client:  FabricApiClient for Fabric REST API calls.
                     If None, all discovery returns empty results gracefully.
            memory:  MemoryManager for audit trail recording.
        """
        self.client = client
        self.memory = memory

    # =========================================================================
    # Public API
    # =========================================================================

    async def build_cascade_plan(
        self,
        workspace_ids: List[str],
        dependency_graphs: Optional[Dict[str, Dict]] = None,
    ) -> ShortcutHealingPlan:
        """
        Discover all broken shortcuts across workspaces, analyze cascade, build plan.

        Runs three phases:
            1. discover_all_shortcuts()   → all shortcut definitions from Fabric API
            2. verify_shortcut_health()   → filter down to broken shortcuts
            3. analyze_cascade()          → downstream impact + fix suggestions

        Returns a ShortcutHealingPlan where:
            auto_actions:   safe to apply immediately (shortcut recreation + model refresh)
            manual_actions: require human sign-off (pipeline config, cross-ws where source gone)
            approval_required: True if any manual_actions exist

        FAANG PARALLEL:
            Same as Meta's "blast radius analysis" — before any infra change,
            automatically compute all downstream effects and group into
            safe-to-automate vs. escalate-to-engineer.

        Args:
            workspace_ids:     List of workspace IDs to scan.
            dependency_graphs: Optional pre-built lineage graphs keyed by workspace_id.
                               If None, cascade analysis falls back to API metadata.
        """
        plan = ShortcutHealingPlan()
        all_shortcuts: List[ShortcutDefinition] = []

        for ws_id in workspace_ids:
            try:
                shortcuts = await self.discover_all_shortcuts(ws_id)
                all_shortcuts.extend(shortcuts)
            except Exception as exc:
                logger.warning(
                    f"ShortcutCascadeManager: failed to discover shortcuts in {ws_id}: {exc}"
                )

        # Verify health of each shortcut (API call per shortcut)
        broken: List[ShortcutDefinition] = []
        for shortcut in all_shortcuts:
            try:
                healthy = await self.verify_shortcut_health(shortcut)
                shortcut.is_healthy = healthy
                if not healthy:
                    broken.append(shortcut)
            except Exception as exc:
                logger.warning(
                    f"ShortcutCascadeManager: health check failed for "
                    f"{shortcut.name} in {shortcut.workspace_id}: {exc}"
                )
                # Treat as broken if we cannot verify
                shortcut.is_healthy = False
                broken.append(shortcut)

        plan.broken_shortcuts = broken

        # Analyze cascade for each broken shortcut
        for shortcut in broken:
            graph = (dependency_graphs or {}).get(shortcut.workspace_id, {})
            impact = await self.analyze_cascade(shortcut, graph)
            plan.cascade_impacts.append(impact)

            # Partition suggestions into auto vs manual
            for suggestion in impact.fix_suggestions:
                if suggestion.auto_applicable:
                    plan.auto_actions.append(suggestion)
                else:
                    plan.manual_actions.append(suggestion)

        plan.approval_required = len(plan.manual_actions) > 0

        logger.info(
            f"ShortcutCascadeManager: plan built — "
            f"{len(broken)} broken shortcuts, "
            f"{len(plan.auto_actions)} auto actions, "
            f"{len(plan.manual_actions)} manual actions"
        )
        return plan

    async def execute_plan(
        self,
        plan: ShortcutHealingPlan,
        dry_run: bool = True,
    ) -> ShortcutHealingResult:
        """
        Execute a ShortcutHealingPlan — recreate shortcuts + refresh models.

        APPROVAL GATE:
            If plan.approval_required and not plan.is_approved:
                Skips all manual_actions (only runs auto_actions).
            If plan.is_approved:
                Runs BOTH auto_actions and manual_actions.

        DRY RUN:
            dry_run=True → all actions are skipped (no API calls made).
            Returns result with skipped=total_actions, applied=0.

        Args:
            plan:    The ShortcutHealingPlan to execute.
            dry_run: True = preview only. False = apply changes.

        Returns:
            ShortcutHealingResult with counts and any errors.
        """
        result = ShortcutHealingResult(plan_id=plan.plan_id, dry_run=dry_run)

        actions_to_run: List[FixSuggestion] = list(plan.auto_actions)
        if plan.is_approved or not plan.approval_required:
            actions_to_run.extend(plan.manual_actions)

        for suggestion in actions_to_run:
            if dry_run:
                result.skipped += 1
                logger.info(
                    f"[DRY RUN] Would execute: {suggestion.action_type} "
                    f"on {suggestion.asset_name} ({suggestion.asset_type})"
                )
                continue

            try:
                success = await self._execute_suggestion(suggestion)
                if success:
                    result.applied += 1
                    logger.info(
                        f"Applied: {suggestion.action_type} on {suggestion.asset_name}"
                    )
                else:
                    result.failed += 1
                    msg = f"Action returned False: {suggestion.action_type} on {suggestion.asset_name}"
                    result.errors.append(msg)
            except Exception as exc:
                result.failed += 1
                msg = f"{suggestion.action_type} on {suggestion.asset_name}: {exc}"
                result.errors.append(msg)
                logger.error(f"ShortcutCascadeManager: heal action failed — {msg}")

        logger.info(
            f"ShortcutCascadeManager: execution complete — "
            f"applied={result.applied}, failed={result.failed}, skipped={result.skipped}"
        )
        return result

    # =========================================================================
    # Discovery
    # =========================================================================

    async def discover_all_shortcuts(
        self, workspace_id: str
    ) -> List[ShortcutDefinition]:
        """
        Query Fabric REST API for ALL shortcuts in a workspace.

        APPROACH:
            1. List all Lakehouse items in the workspace
               GET /v1/workspaces/{ws_id}/items?type=Lakehouse
            2. For each lakehouse, list its shortcuts
               GET /v1/workspaces/{ws_id}/items/{lh_id}/shortcuts
            3. Parse each shortcut's oneLake target → ShortcutDefinition

        FAANG PARALLEL:
            Netflix asset inventory: they periodically crawl all S3 buckets
            to build a complete picture of what exists, what's referenced, and
            what's orphaned. Same "enumerate everything first" pattern.

        Args:
            workspace_id: Workspace to scan.

        Returns:
            List of ShortcutDefinition (one per discovered shortcut).
            Returns [] if client is None or API returns no results.
        """
        if self.client is None:
            return []

        shortcuts: List[ShortcutDefinition] = []

        # Step 1: List all lakehouses in the workspace
        lakehouses = await self._list_lakehouses(workspace_id)

        # Step 2: For each lakehouse, list shortcuts
        for lh in lakehouses:
            lh_id = lh.get("id", "")
            lh_name = lh.get("displayName", "")
            if not lh_id:
                continue

            try:
                raw_shortcuts = await self._list_shortcuts(workspace_id, lh_id)
                for raw in raw_shortcuts:
                    sc = self._parse_shortcut(
                        raw=raw,
                        workspace_id=workspace_id,
                        lakehouse_id=lh_id,
                        lakehouse_name=lh_name,
                    )
                    if sc:
                        shortcuts.append(sc)
            except Exception as exc:
                logger.warning(
                    f"ShortcutCascadeManager: could not list shortcuts for "
                    f"lakehouse {lh_name} ({lh_id}): {exc}"
                )

        logger.info(
            f"ShortcutCascadeManager: discovered {len(shortcuts)} shortcuts "
            f"across {len(lakehouses)} lakehouses in workspace {workspace_id}"
        )
        return shortcuts

    async def verify_shortcut_health(self, shortcut: ShortcutDefinition) -> bool:
        """
        Check if a shortcut's source is still accessible.

        Makes a lightweight API call to the SOURCE workspace/lakehouse.
        A 200 response means the source is accessible → shortcut is healthy.
        A 404 or connection error means the source is gone → shortcut is broken.

        WHY THIS MATTERS:
            We cannot trust the shortcut's own status — Fabric does not update
            shortcut metadata when the source is deleted. We must probe the source
            directly to know if the shortcut is broken.

        Args:
            shortcut: The shortcut to verify.

        Returns:
            True if source is accessible, False if broken.
        """
        if self.client is None:
            return True  # Cannot verify without client — assume healthy

        if not shortcut.source_workspace_id or not shortcut.source_lakehouse_id:
            # Missing source info — cannot verify, treat as unhealthy
            return False

        return await self._source_accessible(
            src_ws_id=shortcut.source_workspace_id,
            src_lh_id=shortcut.source_lakehouse_id,
        )

    # =========================================================================
    # Cascade Analysis
    # =========================================================================

    async def analyze_cascade(
        self,
        broken: ShortcutDefinition,
        graph: Optional[Dict] = None,
    ) -> CascadeImpact:
        """
        Traverse the lineage graph downstream from a broken shortcut.

        LINEAGE TRAVERSAL:
            The lineage graph (built by DiscoveryAgent) stores edges like:
                report-001 → model-001 → table-001 → shortcut-abc

            Starting from the shortcut, we walk *downstream* (to assets that
            DEPEND ON the shortcut) and collect all impacted assets.

        FIX SUGGESTION LOGIC:
            ┌─────────────┬───────────────────┬─────────────┐
            │ Asset Type  │ Action            │ Auto-Apply? │
            ├─────────────┼───────────────────┼─────────────┤
            │ shortcut    │ recreate_shortcut │ Yes         │
            │ sem. model  │ refresh_model     │ Yes         │
            │ pipeline    │ manual_review     │ No          │
            │ report      │ manual_review     │ No          │
            └─────────────┴───────────────────┴─────────────┘

        Shortcuts and model refreshes are safe to auto-apply because they are:
          - Idempotent (recreating an existing shortcut replaces it cleanly)
          - Reversible (can delete the shortcut or revert the model)

        Pipelines and reports require manual review because:
          - Pipeline configs may reference hard-coded paths that need updating
          - Reports may have bookmark states, publish dates, etc.

        FAANG PARALLEL:
            Google's Monarch monitoring system: when a dependency changes,
            it traverses the alerting DAG to find all downstream dashboards
            and sends targeted "your data source changed" notifications.

        Args:
            broken: The broken ShortcutDefinition to start traversal from.
            graph:  Lineage graph dict (from SharedMemory/DiscoveryAgent).
                    If empty, cascade impact is computed from shortcut only.

        Returns:
            CascadeImpact with all impacted assets + FixSuggestions.
        """
        impact = CascadeImpact(shortcut=broken)

        # Suggestion 1: Recreate the broken shortcut itself
        can_recreate = (
            bool(broken.source_workspace_id)
            and bool(broken.source_lakehouse_id)
            and bool(broken.source_path)
        )
        impact.fix_suggestions.append(
            FixSuggestion(
                asset_id=broken.shortcut_id,
                asset_name=broken.name,
                asset_type="shortcut",
                suggestion=(
                    f"Recreate shortcut '{broken.name}' pointing to "
                    f"{broken.source_workspace_id}/{broken.source_lakehouse_id}"
                    f"/{broken.source_path}"
                    if can_recreate
                    else f"Shortcut '{broken.name}' cannot be auto-recreated — "
                         f"source path unknown. Manual intervention required."
                ),
                action_type="recreate_shortcut",
                auto_applicable=can_recreate,
                metadata={
                    "workspace_id": broken.workspace_id,
                    "lakehouse_id": broken.lakehouse_id,
                    "source_workspace_id": broken.source_workspace_id,
                    "source_lakehouse_id": broken.source_lakehouse_id,
                    "source_path": broken.source_path,
                    "shortcut_name": broken.name,
                },
            )
        )

        if not graph:
            return impact

        # Walk the lineage graph to find downstream assets
        nodes: Dict[str, Dict] = {n["id"]: n for n in graph.get("nodes", [])}
        edges: List[Dict] = graph.get("edges", [])

        # Build adjacency: asset_id → set of asset_ids that use it
        dependents: Dict[str, Set[str]] = {}
        for edge in edges:
            src = edge.get("from_id", "")
            dst = edge.get("to_id", "")
            if dst not in dependents:
                dependents[dst] = set()
            dependents[dst].add(src)

        # Find all assets that reference the shortcut (by name match in edges)
        shortcut_consumers: Set[str] = set()
        for edge in edges:
            measures = edge.get("measures", [])
            path = edge.get("path", "")
            if broken.name in measures or broken.name in str(path):
                shortcut_consumers.add(edge.get("from_id", ""))

        # BFS from shortcut consumers → collect all downstream
        visited: Set[str] = set()
        queue = list(shortcut_consumers)
        while queue:
            node_id = queue.pop(0)
            if node_id in visited:
                continue
            visited.add(node_id)

            node = nodes.get(node_id, {})
            node_type = node.get("type", "")
            node_name = node.get("name", node_id)

            if node_type == "report":
                impact.impacted_reports.append(node_name)
                impact.fix_suggestions.append(
                    FixSuggestion(
                        asset_id=node_id,
                        asset_name=node_name,
                        asset_type="report",
                        suggestion=(
                            f"Report '{node_name}' depends on broken shortcut "
                            f"'{broken.name}'. Verify data connections and republish "
                            f"after the shortcut is recreated."
                        ),
                        action_type="manual_review",
                        auto_applicable=False,
                        metadata={"shortcut_name": broken.name},
                    )
                )
            elif node_type == "pipeline":
                impact.impacted_pipelines.append(node_name)
                impact.fix_suggestions.append(
                    FixSuggestion(
                        asset_id=node_id,
                        asset_name=node_name,
                        asset_type="pipeline",
                        suggestion=(
                            f"Pipeline '{node_name}' references broken shortcut "
                            f"'{broken.name}'. Update pipeline source references "
                            f"and re-run after the shortcut is recreated."
                        ),
                        action_type="manual_review",
                        auto_applicable=False,
                        metadata={"shortcut_name": broken.name},
                    )
                )
            elif node_type == "semantic_model":
                impact.impacted_semantic_models.append(node_name)
                impact.fix_suggestions.append(
                    FixSuggestion(
                        asset_id=node_id,
                        asset_name=node_name,
                        asset_type="semantic_model",
                        suggestion=(
                            f"Refresh semantic model '{node_name}' after shortcut "
                            f"'{broken.name}' is recreated."
                        ),
                        action_type="refresh_model",
                        auto_applicable=True,
                        metadata={
                            "model_id": node_id,
                            "workspace_id": broken.workspace_id,
                            "shortcut_name": broken.name,
                        },
                    )
                )
            else:
                # table or unknown — treated as a table alias
                impact.impacted_tables.append(node_name)

            # Continue BFS through consumers of this node
            for consumer_id in dependents.get(node_id, set()):
                if consumer_id not in visited:
                    queue.append(consumer_id)

        return impact

    # =========================================================================
    # Private — Fabric API helpers
    # =========================================================================

    async def _list_lakehouses(self, workspace_id: str) -> List[Dict]:
        """
        GET /v1/workspaces/{ws_id}/items?type=Lakehouse

        Returns list of lakehouse item dicts with "id" and "displayName".
        """
        if self.client is None:
            return []
        try:
            data = await self.client.get(
                f"/workspaces/{workspace_id}/items",
                params={"type": "Lakehouse"},
            )
            return data.get("value", [])
        except Exception as exc:
            logger.warning(
                f"ShortcutCascadeManager: _list_lakehouses({workspace_id}) failed: {exc}"
            )
            return []

    async def _list_shortcuts(
        self, workspace_id: str, lakehouse_id: str
    ) -> List[Dict]:
        """
        GET /v1/workspaces/{ws_id}/items/{lh_id}/shortcuts

        Returns raw shortcut objects from the Fabric API:
            {
              "name": "DimDate",
              "path": "Tables",
              "target": {
                "oneLake": {
                  "workspaceId": "...",
                  "itemId": "...",
                  "path": "Tables/DimDate"
                }
              }
            }
        """
        if self.client is None:
            return []
        try:
            data = await self.client.get(
                f"/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"
            )
            return data.get("value", [])
        except Exception as exc:
            logger.warning(
                f"ShortcutCascadeManager: _list_shortcuts({workspace_id}, {lakehouse_id}) failed: {exc}"
            )
            return []

    async def _source_accessible(
        self, src_ws_id: str, src_lh_id: str
    ) -> bool:
        """
        GET /v1/workspaces/{src_ws_id}/items/{src_lh_id}

        200 = source lakehouse accessible → shortcut is healthy.
        4xx/5xx or exception = source gone → shortcut is broken.
        """
        if self.client is None:
            return True
        try:
            await self.client.get(
                f"/workspaces/{src_ws_id}/items/{src_lh_id}"
            )
            return True
        except Exception:
            return False

    async def _execute_suggestion(self, suggestion: FixSuggestion) -> bool:
        """
        Dispatch a FixSuggestion to the correct executor.

        Routes:
            recreate_shortcut → _recreate_shortcut()
            refresh_model     → _refresh_semantic_model()
            manual_review     → logged but not executed (should not be called)

        Returns True if the action was applied successfully.
        """
        action_type = suggestion.action_type
        meta = suggestion.metadata

        if action_type == "recreate_shortcut":
            return await self._recreate_shortcut(
                workspace_id=meta.get("workspace_id", ""),
                lakehouse_id=meta.get("lakehouse_id", ""),
                shortcut_name=meta.get("shortcut_name", ""),
                source_workspace_id=meta.get("source_workspace_id", ""),
                source_lakehouse_id=meta.get("source_lakehouse_id", ""),
                source_path=meta.get("source_path", ""),
            )
        elif action_type == "refresh_model":
            return await self._refresh_semantic_model(
                workspace_id=meta.get("workspace_id", ""),
                model_id=meta.get("model_id", ""),
            )
        elif action_type == "manual_review":
            logger.warning(
                f"ShortcutCascadeManager: skipping manual_review action "
                f"for {suggestion.asset_name} — requires human sign-off"
            )
            return False
        else:
            logger.warning(
                f"ShortcutCascadeManager: unknown action_type '{action_type}'"
            )
            return False

    async def _recreate_shortcut(
        self,
        workspace_id: str,
        lakehouse_id: str,
        shortcut_name: str,
        source_workspace_id: str,
        source_lakehouse_id: str,
        source_path: str,
    ) -> bool:
        """
        POST /v1/workspaces/{ws_id}/items/{lh_id}/shortcuts

        Creates (or recreates) a OneLake shortcut.

        CRITICAL: The correct endpoint is .../items/{lh_id}/shortcuts
        NOT .../items  (which creates a workspace item, not a shortcut).

        Body format (from Fabric API reference):
            {
              "path": "Tables",
              "name": "DimDate",
              "target": {
                "oneLake": {
                  "workspaceId": "<source-ws-id>",
                  "itemId": "<source-lh-id>",
                  "path": "Tables/DimDate"
                }
              }
            }
        """
        if self.client is None:
            logger.warning("ShortcutCascadeManager: no client — cannot recreate shortcut")
            return False

        # Determine path prefix from source_path (e.g. "Tables/DimDate" → "Tables")
        path_parts = source_path.split("/")
        shortcut_path_prefix = path_parts[0] if path_parts else "Tables"

        try:
            await self.client.post(
                f"/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts",
                json={
                    "path": shortcut_path_prefix,
                    "name": shortcut_name,
                    "target": {
                        "oneLake": {
                            "workspaceId": source_workspace_id,
                            "itemId": source_lakehouse_id,
                            "path": source_path,
                        }
                    },
                },
            )
            logger.info(
                f"ShortcutCascadeManager: recreated shortcut '{shortcut_name}' "
                f"in workspace {workspace_id} / lakehouse {lakehouse_id}"
            )
            return True
        except Exception as exc:
            logger.error(
                f"ShortcutCascadeManager: failed to recreate shortcut '{shortcut_name}': {exc}"
            )
            return False

    async def _refresh_semantic_model(
        self, workspace_id: str, model_id: str
    ) -> bool:
        """
        POST /v1/workspaces/{ws_id}/items/{model_id}/refreshes

        Triggers a full refresh of a semantic model (Power BI dataset).
        Should be called AFTER the broken shortcut has been recreated so
        the model can pick up the restored data.

        Body:
            {"refreshType": "Full"}
        """
        if self.client is None:
            logger.warning("ShortcutCascadeManager: no client — cannot refresh model")
            return False

        try:
            await self.client.post(
                f"/workspaces/{workspace_id}/items/{model_id}/refreshes",
                json={"refreshType": "Full"},
            )
            logger.info(
                f"ShortcutCascadeManager: triggered Full refresh for model {model_id}"
            )
            return True
        except Exception as exc:
            logger.error(
                f"ShortcutCascadeManager: failed to refresh model {model_id}: {exc}"
            )
            return False

    # =========================================================================
    # Private — Parsing
    # =========================================================================

    def _parse_shortcut(
        self,
        raw: Dict,
        workspace_id: str,
        lakehouse_id: str,
        lakehouse_name: str,
    ) -> Optional[ShortcutDefinition]:
        """
        Parse a raw Fabric API shortcut response into a ShortcutDefinition.

        Expected raw format:
            {
              "name": "DimDate",
              "path": "Tables",
              "target": {
                "oneLake": {
                  "workspaceId": "...",
                  "itemId": "...",
                  "path": "Tables/DimDate"
                }
              }
            }

        Returns None if the shortcut is not a OneLake shortcut (e.g. ADLS Gen2
        external shortcuts that cannot be recreated via the same mechanism).
        """
        name = raw.get("name", "")
        if not name:
            return None

        target = raw.get("target", {})
        one_lake = target.get("oneLake", {})

        if not one_lake:
            # External shortcut (ADLS Gen2, S3, GCS) — not handled in this phase
            logger.debug(
                f"ShortcutCascadeManager: skipping non-OneLake shortcut '{name}'"
            )
            return None

        source_path = one_lake.get("path", "")
        # Fabric API returns path relative to lakehouse; normalize to include name
        if not source_path:
            source_path = f"Tables/{name}"

        return ShortcutDefinition(
            name=name,
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            source_workspace_id=one_lake.get("workspaceId", ""),
            source_lakehouse_id=one_lake.get("itemId", ""),
            source_path=source_path,
            is_healthy=True,  # Set by verify_shortcut_health()
        )

    # =========================================================================
    # Enterprise Blast Radius Analysis (Phase I)
    # =========================================================================

    async def analyze_enterprise_blast_radius(
        self,
        source_asset_name: str,
        workspace_ids: List[str],
        change_description: str = "schema change",
    ) -> EnterpriseBlastRadius:
        """
        Compute the full cross-workspace blast radius for a change to a source asset.

        This is the flagship analysis method of Phase I.  It answers the question:
        "If fact_sales changes, what breaks across all workspaces?"

        ALGORITHM:
            1. Build a complete multi-workspace lineage graph via LineageEngine.
            2. Find the source node by name (table, shortcut, or lakehouse).
            3. BFS forward traversal to collect all downstream nodes with depths.
            4. For each downstream node, classify the action type and urgency.
            5. Group impacted assets by workspace → WorkspaceBlastRadius list.
            6. Topological sort all actions into an ordered healing plan.
            7. Compute risk level (critical if 3+ workspaces or 10+ assets).

        ACTION ASSIGNMENT RULES:
            shortcut      → verify_shortcut / recreate_shortcut
            pipeline      → trigger_pipeline (auto, after shortcuts healed)
            semantic_model → refresh_model (auto, after pipeline runs)
            report        → notify_owner (advisory — auto-refreshes with model)
            table         → verify source pipeline integrity

        HEALING ORDER (must follow data flow direction):
            1. Source workspace pipelines (DataPlatform_DEV)
            2. Consumer shortcut verification/recreation
            3. Consumer pipelines (SalesAnalytics_DEV, Finance_DEV)
            4. Semantic models (all workspaces)
            5. Reports (advisory notifications only)

        FAANG PARALLEL:
            - Airbnb Minerva: change impact on downstream metrics + owner notifications
            - LinkedIn DataHub: upstream/downstream lineage traversal for schema changes
            - Google Dataplex: lineage-aware change governance with ordered remediation

        Args:
            source_asset_name:  Name of the table or asset that changed.
            workspace_ids:      All workspace IDs to scan (source + all consumers).
            change_description: Human-readable description of the change.

        Returns:
            EnterpriseBlastRadius with per-workspace details and ordered healing plan.
        """
        from fabric_agent.lineage.engine import LineageEngine
        from fabric_agent.lineage.models import AssetType

        if not self.client:
            return EnterpriseBlastRadius(
                source_asset_name=source_asset_name,
                source_workspace_name="",
                change_description=change_description,
                total_workspaces_impacted=0,
                total_assets_impacted=0,
                risk_level="low",
            )

        # ── Step 1: Build full lineage graph ──────────────────────────────────
        engine = LineageEngine(self.client)
        graph = await engine.build_graph(workspace_ids)

        # ── Step 2: Find source node by name ──────────────────────────────────
        source_node = None
        for node in graph.nodes.values():
            if node.name.lower() == source_asset_name.lower() and node.asset_type in (
                AssetType.TABLE, AssetType.LAKEHOUSE, AssetType.SHORTCUT,
            ):
                source_node = node
                break

        if source_node is None:
            logger.warning(f"Source asset '{source_asset_name}' not found in graph")
            return EnterpriseBlastRadius(
                source_asset_name=source_asset_name,
                source_workspace_name="",
                change_description=change_description,
                total_workspaces_impacted=0,
                total_assets_impacted=0,
                risk_level="low",
            )

        source_ws_name = source_node.workspace_name or ""

        # ── Step 3: BFS forward traversal ─────────────────────────────────────
        downstream: Dict[str, int] = graph.get_all_downstream(source_node.id, max_depth=10)

        # ── Step 4: Build ImpactedAssetDetail for each downstream node ────────
        all_details: List[ImpactedAssetDetail] = []
        for node_id, depth in downstream.items():
            node = graph.get_node(node_id)
            if node is None:
                continue

            # Compute impact path: ancestor names from source to this node
            impact_path = self._compute_impact_path(graph, source_node.id, node_id)

            action_type, action_desc, auto, urgency = self._classify_action(
                node.asset_type.value, depth, source_asset_name,
            )

            detail = ImpactedAssetDetail(
                asset_id=node_id,
                asset_name=node.name,
                asset_type=node.asset_type.value,
                workspace_id=node.workspace_id or "",
                workspace_name=node.workspace_name or "",
                impact_depth=depth,
                impact_path=impact_path,
                action_type=action_type,
                action_description=action_desc,
                auto_applicable=auto,
                urgency=urgency,
            )
            all_details.append(detail)

        # ── Step 5: Group by workspace ────────────────────────────────────────
        ws_map: Dict[str, WorkspaceBlastRadius] = {}
        for detail in all_details:
            ws_id = detail.workspace_id
            if ws_id not in ws_map:
                role = "source" if detail.workspace_name == source_ws_name else "consumer"
                ws_map[ws_id] = WorkspaceBlastRadius(
                    workspace_id=ws_id,
                    workspace_name=detail.workspace_name,
                    role=role,
                )
            ws_map[ws_id].impacted_assets.append(detail)

        per_workspace = sorted(
            ws_map.values(),
            key=lambda w: (0 if w.role == "source" else 1, w.workspace_name),
        )

        # ── Step 6: Ordered healing plan (topological / priority sort) ────────
        priority_order = {
            ActionType.VERIFY_SHORTCUT: 1,
            ActionType.RECREATE_SHORTCUT: 1,
            ActionType.TRIGGER_PIPELINE: 2,
            ActionType.REFRESH_MODEL: 3,
            ActionType.NOTIFY_OWNER: 4,
        }
        ordered = sorted(
            all_details,
            key=lambda d: (
                priority_order.get(d.action_type, 9),
                d.impact_depth,
                d.workspace_name,
            ),
        )

        # ── Step 7: Risk level ────────────────────────────────────────────────
        n_ws = len(ws_map)
        n_assets = len(all_details)
        if n_ws >= 3 or n_assets >= 10:
            risk = "critical"
        elif n_ws >= 2 or n_assets >= 5:
            risk = "high"
        elif n_assets >= 2:
            risk = "medium"
        else:
            risk = "low"

        return EnterpriseBlastRadius(
            source_asset_name=source_asset_name,
            source_workspace_name=source_ws_name,
            change_description=change_description,
            total_workspaces_impacted=n_ws,
            total_assets_impacted=n_assets,
            per_workspace=list(per_workspace),
            ordered_healing_steps=ordered,
            risk_level=risk,
        )

    def _classify_action(
        self, asset_type: str, depth: int, source_name: str,
    ) -> tuple:
        """
        Return (action_type, action_description, auto_applicable, urgency) for an asset.

        Rules mirror how Airbnb Minerva classifies change impact:
        - Shortcuts: verify first (auto), recreate if broken (auto for OneLake)
        - Pipelines: trigger re-run (auto, assumes pipeline is idempotent)
        - Semantic models: refresh (auto, no schema risk at model level)
        - Reports: advisory only (they auto-refresh when model is refreshed)
        """
        if asset_type == "shortcut":
            return (
                ActionType.VERIFY_SHORTCUT,
                f"Verify shortcut pointing to {source_name} is reachable; recreate if broken",
                True,
                Urgency.IMMEDIATE,
            )
        elif asset_type == "pipeline":
            return (
                ActionType.TRIGGER_PIPELINE,
                f"Trigger pipeline re-run after {source_name} changes",
                True,
                Urgency.AFTER_SOURCE_FIXED,
            )
        elif asset_type == "semantic_model":
            return (
                ActionType.REFRESH_MODEL,
                f"Refresh semantic model after upstream {source_name} changes",
                True,
                Urgency.AFTER_SOURCE_FIXED,
            )
        elif asset_type == "report":
            return (
                ActionType.NOTIFY_OWNER,
                f"Notify report owners — model refresh will propagate {source_name} changes automatically",
                False,
                Urgency.ADVISORY,
            )
        elif asset_type in ("table", "lakehouse"):
            return (
                ActionType.TRIGGER_PIPELINE,
                f"Trigger upstream pipeline that writes to {asset_type}",
                True,
                Urgency.IMMEDIATE,
            )
        else:
            return (
                ActionType.NOTIFY_OWNER,
                f"Review {asset_type} for impact from {source_name} change",
                False,
                Urgency.ADVISORY,
            )

    def _compute_impact_path(
        self, graph: "LineageGraph", source_id: str, target_id: str,
    ) -> List[str]:
        """
        BFS shortest path from source_id to target_id, returning node names.
        Used to populate ImpactedAssetDetail.impact_path for display purposes.
        """
        if source_id == target_id:
            node = graph.get_node(source_id)
            return [node.name] if node else [source_id]

        from collections import deque
        visited: Set[str] = {source_id}
        queue: deque = deque([[source_id]])

        while queue:
            path = queue.popleft()
            current = path[-1]
            for edge in graph.edges:
                if edge.source_id == current and edge.target_id not in visited:
                    new_path = path + [edge.target_id]
                    if edge.target_id == target_id:
                        return [
                            (graph.get_node(nid).name if graph.get_node(nid) else nid)
                            for nid in new_path
                        ]
                    visited.add(edge.target_id)
                    queue.append(new_path)

        # Fallback: just return source and target names
        src_node = graph.get_node(source_id)
        tgt_node = graph.get_node(target_id)
        return [
            src_node.name if src_node else source_id,
            tgt_node.name if tgt_node else target_id,
        ]
