#!/usr/bin/env python3
"""Build a workspace dependency graph (JSON).

Usage examples:
  python scripts/build_workspace_graph.py --workspace "fabric-refactor-demo"
  python scripts/build_workspace_graph.py --workspace "fabric-refactor-demo" --out memory/workspace_graph.json

The output can be viewed in the browser with:
  python -m fabric_agent.ui.graph_viewer --graph memory/workspace_graph.json --open
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from loguru import logger

from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.api.fabric_client import FabricApiClient
from fabric_agent.tools.workspace_graph import WorkspaceGraphBuilder


async def _resolve_workspace_id(client: FabricApiClient, workspace_name: str) -> str:
    resp = await client.request("GET", "/workspaces")
    values = resp.get("value") or resp.get("workspaces") or []
    for w in values:
        if str(w.get("name", "")).lower() == workspace_name.lower():
            return str(w.get("id"))
    raise RuntimeError(f"Workspace not found: {workspace_name}")


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    def esc(x: object) -> str:
        return str(x).replace("|", "\\|")

    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for r in rows:
        out.append("| " + " | ".join(esc(c) for c in r) + " |")
    return "\n".join(out)


async def main_async(args: argparse.Namespace) -> int:
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cfg = FabricAuthConfig.from_env()
    async with FabricApiClient(cfg) as client:
        ws_id = args.workspace_id
        if not ws_id:
            if not args.workspace:
                raise SystemExit("Provide --workspace or --workspace-id")
            ws_id = await _resolve_workspace_id(client, args.workspace)

        logger.info(f"Building workspace graph for {ws_id} ...")

        builder = WorkspaceGraphBuilder(
            include_measure_graph=not args.no_measures,
            max_measure_nodes_per_model=args.max_measures,
        )
        graph = await builder.build(client=client, workspace_id=ws_id, workspace_name=args.workspace)

    out_path.write_text(json.dumps(graph, indent=2, ensure_ascii=False), encoding="utf-8")

    stats = graph.get("stats", {})
    print("\n" + _md_table(
        ["Output", "Nodes", "Edges", "Warnings"],
        [[str(out_path), str(stats.get("nodes", "?")), str(stats.get("edges", "?")), str(stats.get("warnings", 0))]],
    ))
    
    if graph.get("warnings"):
        print("\nWarnings:")
        for w in graph.get("warnings", [])[:20]:
            print("-", w)
        if len(graph.get("warnings", [])) > 20:
            print(f"- ... ({len(graph.get('warnings', [])) - 20} more)")

    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build a Fabric workspace impact/dependency graph")
    p.add_argument("--workspace", help="Workspace display name (recommended)")
    p.add_argument("--workspace-id", help="Workspace UUID")
    p.add_argument("--out", default="memory/workspace_graph.json", help="Output JSON path")
    p.add_argument("--no-measures", action="store_true", help="Skip measure-level graph")
    p.add_argument("--max-measures", type=int, default=600, help="Max measure nodes per semantic model")
    return p


def main() -> None:
    args = build_parser().parse_args()
    raise SystemExit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
