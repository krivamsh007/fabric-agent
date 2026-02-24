"""
Fabric Agent CLI
================

Command-line interface for the Fabric Agent.

Usage:
    fabric-agent --help
    fabric-agent list-workspaces
    fabric-agent set-workspace "My Workspace"
    fabric-agent analyze-impact --type measure --name Sales
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Awaitable, Callable, List, Optional, TypeVar

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from fabric_agent.core.agent import FabricAgent
from fabric_agent.tools.models import TargetType

app = typer.Typer(
    name="fabric-agent",
    help="Microsoft Fabric AI Agent CLI",
    add_completion=False,
)
console = Console()


def _esc(text: object) -> str:
    """Escape Rich markup characters in dynamic strings to prevent parse errors."""
    return str(text).replace("[", "\\[").replace("]", "\\]")

# Global agent instance
_agent: Optional[FabricAgent] = None
_T = TypeVar("_T")


def get_agent() -> FabricAgent:
    """Get or create the global agent instance."""
    global _agent
    if _agent is None:
        _agent = FabricAgent(log_level="WARNING")
    return _agent


async def ensure_initialized() -> FabricAgent:
    """Ensure agent is initialized."""
    agent = get_agent()
    if not agent._initialized:
        await agent.initialize()
    return agent


def _exit_code_from_system_exit(code: object) -> int:
    """Normalize SystemExit.code to an integer exit code."""
    if code is None:
        return 0
    if isinstance(code, int):
        return code
    try:
        return int(code)
    except (TypeError, ValueError):
        return 1


async def _close_agent_async() -> None:
    """Best-effort shutdown for the global agent on command completion."""
    global _agent
    agent = _agent
    _agent = None
    if agent is None:
        return
    try:
        await agent.close()
    except Exception:
        # Don't hide command failures if shutdown has a cleanup issue.
        pass


def _run_async(coro_factory: Callable[[], Awaitable[_T]]) -> _T:
    """Run an async command and preserve non-zero exits."""
    async def _runner() -> _T:
        try:
            return await coro_factory()
        finally:
            await _close_agent_async()

    try:
        return asyncio.run(_runner())
    except SystemExit as exc:
        code = _exit_code_from_system_exit(exc.code)
        if code == 0:
            raise
        raise typer.Exit(code=code) from exc


@app.command()
def list_workspaces():
    """List all accessible Fabric workspaces."""
    async def _run():
        agent = await ensure_initialized()
        result = await agent.list_workspaces()
        
        table = Table(title="Fabric Workspaces")
        table.add_column("Name", style="cyan")
        table.add_column("ID", style="dim")
        table.add_column("Type")
        
        for ws in result.workspaces:
            table.add_row(ws.name, ws.id, ws.type)
        
        console.print(table)
        console.print(f"\nTotal: {result.count} workspaces")
    
    _run_async(_run)



@app.command()
def set_workspace(name: str = typer.Argument(..., help="Workspace name")):
    """Set the active workspace."""
    async def _run():
        agent = await ensure_initialized()
        result = await agent.set_workspace(name)
        
        console.print(Panel(
            f"[green]✓[/green] Active workspace: [cyan]{result.workspace_name}[/cyan]\n"
            f"ID: {result.workspace_id}",
            title="Workspace Set",
        ))
    
    _run_async(_run)



@app.command()
def list_items(
    item_type: Optional[str] = typer.Option(
        None, "--type", "-t",
        help="Filter by type (Report, SemanticModel, etc.)"
    )
):
    """List items in the current workspace."""
    async def _run():
        agent = await ensure_initialized()
        result = await agent.list_items(item_type)
        
        table = Table(title="Workspace Items")
        table.add_column("Name", style="cyan")
        table.add_column("Type", style="magenta")
        table.add_column("ID", style="dim")
        
        for item in result.items:
            table.add_row(item.name, item.type, item.id)
        
        console.print(table)
        console.print(f"\nTotal: {result.count} items")
    
    _run_async(_run)



@app.command()
def get_measures(model: str = typer.Argument(..., help="Semantic model name")):
    """Get measures from a semantic model."""
    async def _run():
        agent = await ensure_initialized()
        result = await agent.get_measures(model)
        
        table = Table(title=f"Measures in {result.model_name}")
        table.add_column("Name", style="cyan")
        table.add_column("Table")
        
        for m in result.measures:
            table.add_row(m.name, m.table or "-")
        
        console.print(table)
        console.print(f"\nTotal: {result.count} measures")
    
    _run_async(_run)



@app.command()
def analyze_impact(
    target_type: str = typer.Option(
        ..., "--type", "-t",
        help="Target type: measure or column"
    ),
    name: str = typer.Option(..., "--name", "-n", help="Target name"),
    model: Optional[str] = typer.Option(
        None, "--model", "-m",
        help="Semantic model name (for DAX analysis)"
    ),
):
    """Analyze impact of renaming a measure or column."""
    async def _run():
        agent = await ensure_initialized()
        result = await agent.analyze_impact(target_type, name, model)
        
        # Summary panel
        severity_colors = {
            "none": "green",
            "low": "yellow",
            "medium": "orange1",
            "high": "red",
            "critical": "bold red",
        }
        color = severity_colors.get(result.severity.value, "white")
        
        console.print(Panel(
            f"Target: [cyan]{result.target_name}[/cyan] ({result.target_type})\n"
            f"Severity: [{color}]{result.severity.value.upper()}[/{color}]\n"
            f"Total Impact: {result.total_impact} items\n"
            f"Safe to Rename: {'✓ Yes' if result.safe_to_rename else '✗ No'}",
            title="Impact Analysis",
        ))
        
        # Affected reports
        if result.affected_reports:
            console.print("\n[bold]Affected Reports:[/bold]")
            for report in result.affected_reports:
                console.print(f"  • {report}")
        
        # Affected visuals
        if result.affected_visuals:
            table = Table(title="Affected Visuals")
            table.add_column("Report")
            table.add_column("Page")
            table.add_column("Visual")
            table.add_column("Type")
            
            for v in result.affected_visuals[:10]:
                table.add_row(v.report, v.page, v.visual_name or "-", v.visual_type or "-")
            
            console.print(table)
            if len(result.affected_visuals) > 10:
                console.print(f"  ... and {len(result.affected_visuals) - 10} more")
        
        # DAX dependencies
        if result.affected_measures:
            console.print("\n[bold]DAX Dependencies:[/bold]")
            for m in result.affected_measures:
                console.print(f"  • {m.measure_name} ({m.dependency_type})")
    
    _run_async(_run)



@app.command()
def history(
    limit: int = typer.Option(10, "--limit", "-l", help="Max events"),
    operation: Optional[str] = typer.Option(
        None, "--operation", "-o",
        help="Filter by operation type"
    ),
):
    """Show refactoring history from audit trail."""
    async def _run():
        agent = await ensure_initialized()
        result = await agent.get_history(limit, operation)
        
        table = Table(title="Refactoring History")
        table.add_column("Timestamp", style="dim")
        table.add_column("Operation", style="cyan")
        table.add_column("Target")
        table.add_column("Change")
        table.add_column("Status")
        
        for event in result.events:
            change = ""
            if event.old_value and event.new_value:
                change = f"{event.old_value} → {event.new_value}"
            
            status_style = "green" if event.status == "success" else "yellow"
            
            table.add_row(
                event.timestamp[:19],
                event.operation,
                event.target or "-",
                change or "-",
                f"[{status_style}]{event.status}[/{status_style}]",
            )
        
        console.print(table)
    
    _run_async(_run)



@app.command()
def status():
    """Check connection status."""
    async def _run():
        agent = await ensure_initialized()
        result = await agent.get_status()
        
        if result.connected:
            console.print(Panel(
                f"[green]✓ Connected[/green]\n"
                f"Workspace: {result.workspace_name or '(not set)'}\n"
                f"Auth Mode: {result.auth_mode}",
                title="Connection Status",
            ))
        else:
            console.print(Panel(
                f"[red]✗ Not Connected[/red]\n"
                f"Error: {result.error}",
                title="Connection Status",
            ))
    
    _run_async(_run)



@app.command()
def version():
    """Show version information."""
    from fabric_agent import __version__
    console.print(f"Fabric Agent v{__version__}")


# ============================================================================
# Self-Healing Infrastructure Commands
# ============================================================================


@app.command("health-scan")
def health_scan(
    workspace_ids: str = typer.Argument(
        ...,
        help="Comma-separated workspace IDs to scan (e.g. ws-abc,ws-def)",
    ),
    stale_hours: int = typer.Option(
        24, "--stale-hours", help="Flag tables not refreshed within N hours"
    ),
):
    """Scan workspace(s) for infrastructure anomalies (broken shortcuts, schema drift, etc.)."""
    async def _run():
        from fabric_agent.tools.models import ScanWorkspaceHealthInput

        ids = [w.strip() for w in workspace_ids.split(",") if w.strip()]
        agent = await ensure_initialized()
        if not agent.tools:
            console.print("[red]Agent tools not initialized.[/red]")
            return

        with console.status("[bold cyan]Scanning workspace health...[/bold cyan]"):
            result = await agent.tools.scan_workspace_health(
                ScanWorkspaceHealthInput(workspace_ids=ids, stale_hours=stale_hours)
            )

        severity_colors = {
            "critical": "bold red",
            "high": "red",
            "medium": "yellow",
            "low": "green",
        }

        table = Table(title=f"Health Scan — {len(ids)} workspace(s)")
        table.add_column("Severity", style="bold")
        table.add_column("Type", style="cyan")
        table.add_column("Asset")
        table.add_column("Can Auto-Heal")
        table.add_column("Details")

        for anomaly in result.anomalies:
            color = severity_colors.get(anomaly.severity, "white")
            table.add_row(
                f"[{color}]{anomaly.severity.upper()}[/{color}]",
                anomaly.anomaly_type,
                anomaly.asset_name,
                "✓" if anomaly.can_auto_heal else "✗",
                (anomaly.details or "-")[:80],
            )

        console.print(table)
        console.print(
            f"\nTotal: [bold]{result.total_found}[/bold] anomalies | "
            f"Auto-healable: [green]{result.auto_healable_count}[/green] | "
            f"[dim]{result.message}[/dim]"
        )

    _run_async(_run)



@app.command("heal")
def heal(
    workspace_ids: str = typer.Argument(
        ...,
        help="Comma-separated workspace IDs (e.g. ws-abc,ws-def)",
    ),
    dry_run: bool = typer.Option(
        True, "--dry-run/--apply",
        help="Simulate (default) or apply healing actions",
    ),
):
    """Execute self-healing for workspace anomalies. Use --apply to actually fix issues."""
    async def _run():
        from fabric_agent.tools.models import ExecuteHealingPlanInput

        ids = [w.strip() for w in workspace_ids.split(",") if w.strip()]
        agent = await ensure_initialized()
        if not agent.tools:
            console.print("[red]Agent tools not initialized.[/red]")
            return

        mode = "[dim](dry run)[/dim]" if dry_run else "[bold red](LIVE)[/bold red]"
        with console.status(f"[bold cyan]Running self-healing {mode}...[/bold cyan]"):
            result = await agent.tools.execute_healing_plan(
                ExecuteHealingPlanInput(workspace_ids=ids, dry_run=dry_run)
            )

        status_color = "green" if result.success else "red"
        console.print(Panel(
            f"[{status_color}]{'✓' if result.success else '✗'} {result.message}[/{status_color}]\n\n"
            f"Applied:  [green]{result.applied}[/green]\n"
            f"Skipped:  [yellow]{result.skipped}[/yellow]\n"
            f"Failed:   [red]{result.failed}[/red]\n"
            f"Dry Run:  {'Yes' if result.dry_run else 'No'}",
            title="Self-Healing Result",
        ))

        if result.errors:
            console.print("\n[bold red]Errors:[/bold red]")
            for err in result.errors:
                console.print(f"  • {_esc(err)}")

    _run_async(_run)



# ============================================================================
# Phase E — Shortcut Cascade Commands
# ============================================================================


@app.command("shortcut-scan")
def shortcut_scan(
    workspace_ids: str = typer.Argument(
        ...,
        help="Comma-separated workspace IDs to scan (e.g. ws-abc,ws-def)",
    ),
):
    """
    Discover broken OneLake shortcuts + their cascade of impacted assets.

    Queries the Fabric REST API for actual shortcut definitions, verifies each
    source is accessible, then shows which downstream tables / semantic models /
    pipelines / reports are affected.

    Run this before shortcut-heal to review what's broken.
    """
    async def _run():
        from fabric_agent.tools.models import ScanShortcutCascadeInput
        from rich.table import Table

        ids = [w.strip() for w in workspace_ids.split(",") if w.strip()]
        agent = await ensure_initialized()
        if not agent.tools:
            console.print("[red]Agent tools not initialized.[/red]")
            return

        with console.status("[bold cyan]Scanning for broken shortcuts...[/bold cyan]"):
            result = await agent.tools.scan_shortcut_cascade(
                ScanShortcutCascadeInput(workspace_ids=ids)
            )

        if result.broken_shortcut_count == 0:
            console.print(
                Panel("[green]No broken shortcuts found.[/green]", title="Shortcut Scan")
            )
            return

        # Summary panel
        console.print(Panel(
            f"[red]{result.broken_shortcut_count}[/red] broken shortcut(s) found\n"
            f"[yellow]{result.total_cascade_impact}[/yellow] downstream asset(s) impacted",
            title="Shortcut Scan Summary",
        ))

        # Broken shortcuts table
        sc_table = Table(title="Broken Shortcuts", show_header=True)
        sc_table.add_column("Name", style="red")
        sc_table.add_column("Workspace")
        sc_table.add_column("Source WS ID")
        sc_table.add_column("Source Path")
        for sc in result.broken_shortcuts:
            sc_table.add_row(
                sc.name,
                sc.workspace_id,
                sc.source_workspace_id or "[dim]unknown[/dim]",
                sc.source_path or "[dim]unknown[/dim]",
            )
        console.print(sc_table)

        # Cascade impact table
        if result.cascade_impacts:
            ci_table = Table(title="Cascade Impact", show_header=True)
            ci_table.add_column("Shortcut")
            ci_table.add_column("Tables", justify="right")
            ci_table.add_column("Semantic Models", justify="right")
            ci_table.add_column("Pipelines", justify="right")
            ci_table.add_column("Reports", justify="right")
            for ci in result.cascade_impacts:
                ci_table.add_row(
                    ci.shortcut_name,
                    str(len(ci.impacted_tables)),
                    str(len(ci.impacted_semantic_models)),
                    str(len(ci.impacted_pipelines)),
                    str(len(ci.impacted_reports)),
                )
            console.print(ci_table)

        console.print(
            "\n[cyan]Run 'fabric-agent shortcut-heal' to build a healing plan.[/cyan]"
        )

    _run_async(_run)



@app.command("shortcut-heal")
def shortcut_heal(
    workspace_ids: str = typer.Argument(
        ...,
        help="Comma-separated workspace IDs (e.g. ws-abc,ws-def)",
    ),
    dry_run: bool = typer.Option(
        True, "--dry-run/--apply",
        help="Simulate (default) or apply healing actions",
    ),
    approved_by: str = typer.Option(
        "cli-user", "--approved-by", help="Approver name for audit trail",
    ),
):
    """
    Build a shortcut cascade healing plan and optionally execute it.

    WORKFLOW:
      1. Builds cascade plan (auto vs manual action split)
      2. Shows the plan for review
      3. If --apply: asks for confirmation, approves, then executes

    Always run without --apply first to review what will happen.
    """
    async def _run():
        from fabric_agent.tools.models import (
            BuildShortcutHealingPlanInput,
            ApproveShortcutHealingInput,
            ExecuteShortcutHealingInput,
        )
        from rich.table import Table

        ids = [w.strip() for w in workspace_ids.split(",") if w.strip()]
        agent = await ensure_initialized()
        if not agent.tools:
            console.print("[red]Agent tools not initialized.[/red]")
            return

        with console.status("[bold cyan]Building shortcut healing plan...[/bold cyan]"):
            plan_result = await agent.tools.build_shortcut_healing_plan(
                BuildShortcutHealingPlanInput(workspace_ids=ids)
            )

        # Display plan
        console.print(Panel(
            f"Plan ID: [bold]{plan_result.plan_id}[/bold]\n"
            f"Auto actions:   [green]{plan_result.auto_action_count}[/green] (safe to apply)\n"
            f"Manual actions: [yellow]{plan_result.manual_action_count}[/yellow] (need approval)\n"
            f"Approval required: {'[red]YES[/red]' if plan_result.approval_required else '[green]No[/green]'}",
            title="Shortcut Healing Plan",
        ))

        if plan_result.auto_actions:
            auto_table = Table(title="Auto Actions (safe to apply)")
            auto_table.add_column("Asset")
            auto_table.add_column("Type")
            auto_table.add_column("Action")
            auto_table.add_column("Suggestion")
            for a in plan_result.auto_actions:
                auto_table.add_row(
                    a.asset_name, a.asset_type, a.action_type,
                    a.suggestion[:60] + "..." if len(a.suggestion) > 60 else a.suggestion,
                )
            console.print(auto_table)

        if plan_result.manual_actions:
            manual_table = Table(title="Manual Actions (need approval)", style="yellow")
            manual_table.add_column("Asset")
            manual_table.add_column("Type")
            manual_table.add_column("Suggestion")
            for a in plan_result.manual_actions:
                manual_table.add_row(
                    a.asset_name, a.asset_type,
                    a.suggestion[:70] + "..." if len(a.suggestion) > 70 else a.suggestion,
                )
            console.print(manual_table)

        if not dry_run:
            mode_label = "[bold red](LIVE)[/bold red]"
            confirmed = typer.confirm(
                f"Apply all auto actions? {len(plan_result.auto_actions)} actions will be executed.",
                default=False,
            )
            if not confirmed:
                console.print("[yellow]Healing cancelled.[/yellow]")
                return

            # Approve if plan has manual actions
            if plan_result.approval_required:
                await agent.tools.approve_shortcut_healing(
                    ApproveShortcutHealingInput(
                        plan_id=plan_result.plan_id,
                        approved_by=approved_by,
                    )
                )
                console.print(f"[green]Plan approved by {approved_by}.[/green]")

            with console.status(f"[bold cyan]Executing healing {mode_label}...[/bold cyan]"):
                exec_result = await agent.tools.execute_shortcut_healing(
                    ExecuteShortcutHealingInput(
                        plan_id=plan_result.plan_id,
                        dry_run=False,
                    )
                )

            status_color = "green" if exec_result.success else "red"
            console.print(Panel(
                f"[{status_color}]{'✓' if exec_result.success else '✗'} {exec_result.message}[/{status_color}]\n\n"
                f"Applied:  [green]{exec_result.applied}[/green]\n"
                f"Skipped:  [yellow]{exec_result.skipped}[/yellow]\n"
                f"Failed:   [red]{exec_result.failed}[/red]",
                title="Shortcut Healing Result",
            ))

            if exec_result.errors:
                console.print("\n[bold red]Errors:[/bold red]")
                for err in exec_result.errors:
                    console.print(f"  • {_esc(err)}")
        else:
            console.print(
                "\n[dim]Dry run: no changes made. "
                "Use --apply to execute.[/dim]"
            )

    _run_async(_run)



# ============================================================================
# Memory / RAG Commands
# ============================================================================

@app.command("memory-stats")
def memory_stats():
    """Show vector store statistics (total indexed, collection size, embedding model)."""
    async def _run():
        from fabric_agent.tools.models import MemoryStatsInput

        agent = await ensure_initialized()
        if not agent.tools:
            console.print("[red]Agent tools not initialized.[/red]")
            return

        result = await agent.tools.memory_stats(MemoryStatsInput())

        table = Table(title="Operation Memory Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value")
        table.add_row("Total Indexed", str(result.total_indexed))
        table.add_row("Vector Store Size", str(result.collection_size))
        table.add_row("Last Indexed", result.last_indexed or "never")
        table.add_row("Embedding Model", result.embedding_model)
        table.add_row("Store Path", result.vector_store_path)

        console.print(table)
        console.print(f"\n[dim]{result.message}[/dim]")

    _run_async(_run)



@app.command("memory-search")
def memory_search(
    query: str = typer.Argument(..., help="Natural language description of the proposed change"),
    top_k: int = typer.Option(5, "--top-k", "-k", help="Number of similar operations to return"),
    change_type: Optional[str] = typer.Option(
        None, "--type", "-t", help="Filter by change type (rename_measure, etc.)"
    ),
):
    """Find past operations semantically similar to a proposed change (vector RAG search)."""
    async def _run():
        from fabric_agent.tools.models import FindSimilarOperationsInput

        agent = await ensure_initialized()
        if not agent.tools:
            console.print("[red]Agent tools not initialized.[/red]")
            return

        result = await agent.tools.find_similar_operations(
            FindSimilarOperationsInput(
                proposed_change=query,
                top_k=top_k,
                change_type=change_type,
            )
        )

        if not result.similar_operations:
            console.print(Panel(
                f"No similar operations found.\n\n[dim]{result.message}[/dim]",
                title="Memory Search",
            ))
            return

        table = Table(title=f"Similar Past Operations (query: '{query[:50]}')")
        table.add_column("Similarity", style="cyan", justify="right")
        table.add_column("Operation", style="magenta")
        table.add_column("Target")
        table.add_column("Outcome")
        table.add_column("Key Lesson")

        for op in result.similar_operations:
            outcome_color = "green" if op.outcome == "success" else "red"
            table.add_row(
                f"{op.similarity:.2%}",
                op.operation,
                op.target_name or "-",
                f"[{outcome_color}]{op.outcome}[/{outcome_color}]",
                (op.key_lesson or "-")[:60],
            )

        console.print(table)
        console.print(f"\nFound [bold]{result.total_found}[/bold] similar operations.")

    _run_async(_run)



@app.command("memory-reindex")
def memory_reindex():
    """Rebuild the vector index from the SQLite audit trail (run after first setup)."""
    async def _run():
        from fabric_agent.tools.models import ReindexMemoryInput

        agent = await ensure_initialized()
        if not agent.tools:
            console.print("[red]Agent tools not initialized.[/red]")
            return

        with console.status("[bold cyan]Reindexing operation memory...[/bold cyan]"):
            result = await agent.tools.reindex_operation_memory(ReindexMemoryInput())

        console.print(Panel(
            f"[green]✓[/green] {result.message}",
            title="Memory Reindex",
        ))

    _run_async(_run)



@app.command("session-summary")
def session_summary(
    workspace_id: Optional[str] = typer.Option(
        None, "--workspace", "-w", help="Workspace ID (defaults to active workspace)"
    ),
):
    """Show what the agent did in the last session (cross-session memory)."""
    async def _run():
        from fabric_agent.tools.models import GetSessionSummaryInput

        agent = await ensure_initialized()
        if not agent.tools:
            console.print("[red]Agent tools not initialized.[/red]")
            return

        result = await agent.tools.get_session_summary(
            GetSessionSummaryInput(workspace_id=workspace_id)
        )

        if result.has_history:
            console.print(Panel(
                result.summary,
                title=f"Last Session Summary (workspace: {result.workspace_id or 'default'})",
            ))
        else:
            console.print(Panel(
                "[dim]No previous sessions found.[/dim]",
                title="Session Summary",
            ))

    _run_async(_run)



# ============================================================================
# Phase I — Enterprise Blast Radius Command
# ============================================================================


@app.command("blast-radius")
def blast_radius(
    source_table: str = typer.Option(
        ..., "--source-table", "-s",
        help="Name of the source asset that changed (e.g. 'fact_sales')",
    ),
    workspace_ids: List[str] = typer.Option(
        ..., "--workspace-id", "-w",
        help="Workspace ID to scan (repeat for multiple workspaces)",
    ),
    change_description: str = typer.Option(
        "schema change", "--change",
        help="Description of the change (e.g. 'column dropped: customer_tier')",
    ),
    json_out: Optional[str] = typer.Option(
        None, "--json-out",
        help="Write full blast-radius JSON to this file path (consumed by execute-cascade-plan)",
    ),
    fail_on_risk: Optional[str] = typer.Option(
        None, "--fail-on-risk",
        help="Exit with code 1 if risk_level >= this level (high|critical). Used by CI to block PRs.",
    ),
):
    """
    Compute the full cross-workspace cascade blast radius for a source asset change.

    Shows which assets across all consumer workspaces will be impacted, and
    provides a topologically ordered healing plan.

    Pass --json-out to save the result for use with execute-cascade-plan.
    Pass --fail-on-risk critical to block CI when a critical schema change is detected.

    Example:
        fabric-agent blast-radius --source-table fact_sales \\
            --workspace-id ws-dataplatform \\
            --workspace-id ws-salesanalytics \\
            --workspace-id ws-finance \\
            --json-out blast_radius.json \\
            --fail-on-risk critical
    """
    _ci_exit: list[int] = [0]  # mutable container — async context signals exit code here

    async def _run():
        from fabric_agent.tools.models import EnterpriseBlastRadiusInput
        from rich.table import Table

        agent = await ensure_initialized()
        if not agent.tools:
            console.print("[red]Agent tools not initialized.[/red]")
            return

        try:
            with console.status(
                f"[bold cyan]Computing blast radius for '{source_table}'...[/bold cyan]"
            ):
                result = await agent.tools.get_enterprise_blast_radius(
                    EnterpriseBlastRadiusInput(
                        source_asset_name=source_table,
                        workspace_ids=list(workspace_ids),
                        change_description=change_description,
                    )
                )

            # ── Summary panel ──────────────────────────────────────────────────
            risk_color = {
                "critical": "red", "high": "red", "medium": "yellow", "low": "green"
            }.get(result.risk_level, "white")

            console.print(Panel(
                f"Source: [bold]{result.source_asset_name}[/bold]"
                f" in [dim]{result.source_workspace_name}[/dim]\n"
                f"Change: {result.change_description}\n\n"
                f"Workspaces impacted: [bold]{result.total_workspaces_impacted}[/bold]\n"
                f"Assets impacted:     [bold]{result.total_assets_impacted}[/bold]\n"
                f"Risk level:          [{risk_color}]{result.risk_level.upper()}[/{risk_color}]",
                title="Enterprise Blast Radius Summary",
            ))

            # ── Per-workspace breakdown ────────────────────────────────────────
            for ws in result.per_workspace:
                role_style = "bold green" if ws.role == "source" else "bold yellow"
                ws_table = Table(
                    title=f"{ws.workspace_name} [{ws.role.upper()}]",
                    title_style=role_style,
                    show_header=True,
                )
                ws_table.add_column("Depth", width=5)
                ws_table.add_column("Asset", style="bold")
                ws_table.add_column("Type")
                ws_table.add_column("Action")
                ws_table.add_column("Auto", width=5)
                ws_table.add_column("Urgency")

                for asset in ws.impacted_assets:
                    auto_marker = "[green]✓[/green]" if asset.auto_applicable else "[red]✗[/red]"
                    urgency_color = (
                        "red" if asset.urgency == "immediate"
                        else "yellow" if asset.urgency == "after_source_fixed"
                        else "dim"
                    )
                    ws_table.add_row(
                        str(asset.impact_depth),
                        asset.asset_name,
                        asset.asset_type,
                        asset.action_type,
                        auto_marker,
                        f"[{urgency_color}]{asset.urgency}[/{urgency_color}]",
                    )
                console.print(ws_table)

            # ── Ordered healing plan ───────────────────────────────────────────
            if result.ordered_healing_steps:
                plan_table = Table(title="Ordered Healing Plan", show_header=True)
                plan_table.add_column("#", width=4)
                plan_table.add_column("Action")
                plan_table.add_column("Asset")
                plan_table.add_column("Workspace")
                plan_table.add_column("Auto", width=5)

                for i, step in enumerate(result.ordered_healing_steps, 1):
                    auto_marker = "[green]✓[/green]" if step.auto_applicable else "[red]✗[/red]"
                    plan_table.add_row(
                        str(i),
                        step.action_type,
                        step.asset_name,
                        step.workspace_name,
                        auto_marker,
                    )
                console.print(plan_table)

            # ── CI/CD outputs ──────────────────────────────────────────────────
            if json_out:
                out_path = Path(json_out)
                out_path.write_text(result.model_dump_json(indent=2))
                console.print(f"[dim]Blast radius JSON written to {out_path}[/dim]")

            if fail_on_risk:
                risk_order = {"low": 0, "medium": 1, "high": 2, "critical": 3}
                actual = risk_order.get(result.risk_level.lower(), 0)
                threshold = risk_order.get(fail_on_risk.lower(), 99)
                if actual >= threshold:
                    console.print(
                        f"[red]FAIL: risk_level={result.risk_level.upper()} "
                        f">= --fail-on-risk={fail_on_risk.upper()}. "
                        f"Blocking CI.[/red]"
                    )
                    _ci_exit[0] = 1  # signal failure without raising in async context
        finally:
            pass

    _run_async(_run)

    if _ci_exit[0]:
        raise typer.Exit(code=_ci_exit[0])


# ============================================================================
# Phase E — Execute Cascade Healing Plan
# ============================================================================


@app.command("execute-cascade-plan")
def execute_cascade_plan(
    plan_json: str = typer.Option(
        ..., "--plan-json", "-p",
        help="Path to blast-radius JSON file produced by `blast-radius --json-out`",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Print what would be executed without calling Fabric APIs",
    ),
    skip_advisory: bool = typer.Option(
        True, "--skip-advisory/--include-advisory",
        help="Skip advisory (notify_owner) steps that don't require API calls (default: skip)",
    ),
):
    """
    Execute the ordered cascade healing plan produced by `blast-radius --json-out`.

    Reads the blast-radius JSON, iterates the topologically sorted healing steps,
    and triggers pipelines / refreshes semantic models in the correct dependency order.

    This command is the GitOps Stage 2: it runs ONLY after a human reviewer approves
    the `fabric-production` GitHub Environment in the fabric-auto-heal workflow.

    Healing order (mirrors data flow, source before consumers):
        1. Source pipelines (DataPlatform_DEV)
        2. Consumer pipelines (SalesAnalytics_DEV, Finance_DEV)
        3. Semantic models (all workspaces)
        4. Reports: advisory notifications (skipped by default)

    FAANG PARALLEL:
        Meta's Dataswarm and LinkedIn's DataHub both use a topological executor
        for cascade remediation: steps are sorted by dependency depth and executed
        layer by layer, with each layer waiting for the previous to succeed before
        triggering the next.  We adopt the same pattern here.

    Example:
        fabric-agent execute-cascade-plan --plan-json blast_radius.json
        fabric-agent execute-cascade-plan --plan-json blast_radius.json --dry-run
        fabric-agent execute-cascade-plan --plan-json blast_radius.json --include-advisory
    """
    async def _run() -> int:
        from fabric_agent.tools.models import EnterpriseBlastRadiusOutput
        from fabric_agent.healing.healer import SelfHealer

        # Load the blast-radius plan produced by Stage 1.
        plan_path = Path(plan_json)
        if not plan_path.exists():
            console.print(f"[red]Plan file not found: {plan_path}[/red]")
            return 1
        try:
            data = json.loads(plan_path.read_text())
            plan = EnterpriseBlastRadiusOutput.model_validate(data)
        except Exception as exc:
            console.print(f"[red]Failed to parse plan JSON: {_esc(exc)}[/red]")
            return 1

        console.print(Panel(
            f"Source: [bold]{plan.source_asset_name}[/bold] in [dim]{plan.source_workspace_name}[/dim]\n"
            f"Change: {plan.change_description}\n"
            f"Steps:  {len(plan.ordered_healing_steps)} total"
            + (" [bold yellow](DRY RUN)[/bold yellow]" if dry_run else ""),
            title="Execute Cascade Healing Plan",
        ))

        agent = await ensure_initialized()
        healer = SelfHealer(client=agent.client if hasattr(agent, "client") else None)

        failed = 0
        skipped = 0
        executed = 0

        for i, step in enumerate(plan.ordered_healing_steps, 1):
            label = f"[{i}/{len(plan.ordered_healing_steps)}] {step.action_type} → {step.asset_name} ({step.workspace_name})"

            # Skip advisory steps (notify_owner) unless --include-advisory
            if step.urgency == "advisory" and skip_advisory:
                console.print(f"  [dim]SKIP (advisory) {label}[/dim]")
                skipped += 1
                continue

            # Skip steps not flagged as auto_applicable (require manual intervention)
            if not step.auto_applicable:
                console.print(f"  [yellow]SKIP (manual) {label}[/yellow]")
                skipped += 1
                continue

            if dry_run:
                console.print(f"  [cyan]DRY RUN {label}[/cyan]")
                executed += 1
                continue

            # Execute the healing action.
            try:
                if step.action_type == "trigger_pipeline":
                    await healer.trigger_pipeline(step.asset_id, step.workspace_id)
                    console.print(f"  [green]OK[/green] {label}")
                    executed += 1
                elif step.action_type == "refresh_model":
                    await healer.refresh_semantic_model(step.asset_id, step.workspace_id)
                    console.print(f"  [green]OK[/green] {label}")
                    executed += 1
                else:
                    # recreate_shortcut / verify_shortcut — log as advisory for now
                    console.print(f"  [dim]SKIP (unsupported action) {label}[/dim]")
                    skipped += 1
            except Exception as exc:
                console.print(f"  [red]FAIL {_esc(label)}: {_esc(exc)}[/red]")
                failed += 1

        # Summary
        status = "[green]COMPLETE[/green]" if failed == 0 else f"[red]FAILED ({failed} errors)[/red]"
        console.print(Panel(
            f"Executed: {executed}  Skipped: {skipped}  Failed: {failed}\n"
            f"Status:   {status}",
            title="Cascade Execution Result",
        ))

        return 1 if failed else 0

    _exit_code = _run_async(_run)

    if _exit_code:
        raise typer.Exit(code=_exit_code)


def main():
    """Main entry point."""
    app()


if __name__ == "__main__":
    main()
