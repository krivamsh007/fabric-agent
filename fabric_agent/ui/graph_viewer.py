"""
Graph Viewer UI
===============

A browser-based interactive viewer for the workspace dependency graph.

Usage:
    python -m fabric_agent.ui.graph_viewer --graph memory/workspace_graph.json --open
    python -m fabric_agent.ui.graph_viewer --graph memory/workspace_graph.json --workspace "my-workspace" --open
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from threading import Thread
from typing import Any, Dict, Optional

from loguru import logger


# HTML template with D3.js visualization
VIEWER_HTML = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Fabric Workspace Dependency Graph</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a2e;
            color: #eee;
            overflow: hidden;
        }
        
        #header {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            height: 60px;
            background: #16213e;
            border-bottom: 1px solid #0f3460;
            display: flex;
            align-items: center;
            padding: 0 20px;
            z-index: 1000;
        }
        
        #header h1 {
            font-size: 18px;
            font-weight: 500;
            color: #e94560;
        }
        
        #header .stats {
            margin-left: auto;
            font-size: 13px;
            color: #888;
        }
        
        #header .stats span {
            margin-left: 20px;
            color: #aaa;
        }
        
        #header button {
            margin-left: 20px;
            padding: 8px 16px;
            background: #0f3460;
            border: 1px solid #e94560;
            color: #e94560;
            border-radius: 4px;
            cursor: pointer;
            font-size: 13px;
        }
        
        #header button:hover {
            background: #e94560;
            color: white;
        }
        
        #graph-container {
            position: fixed;
            top: 60px;
            left: 0;
            right: 300px;
            bottom: 0;
        }
        
        #sidebar {
            position: fixed;
            top: 60px;
            right: 0;
            width: 300px;
            bottom: 0;
            background: #16213e;
            border-left: 1px solid #0f3460;
            padding: 20px;
            overflow-y: auto;
        }
        
        #sidebar h2 {
            font-size: 14px;
            color: #e94560;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        #sidebar .section {
            margin-bottom: 25px;
        }
        
        #sidebar .item {
            padding: 8px 12px;
            background: #1a1a2e;
            border-radius: 4px;
            margin-bottom: 8px;
            font-size: 13px;
        }
        
        #sidebar .item .label {
            color: #888;
            font-size: 11px;
            text-transform: uppercase;
        }
        
        #sidebar .item .value {
            color: #eee;
            margin-top: 2px;
        }
        
        svg {
            width: 100%;
            height: 100%;
        }
        
        .node {
            cursor: pointer;
        }
        
        .node circle {
            stroke: #fff;
            stroke-width: 1.5px;
        }
        
        .node text {
            font-size: 11px;
            fill: #eee;
            pointer-events: none;
        }
        
        .link {
            stroke: #0f3460;
            stroke-opacity: 0.6;
            fill: none;
        }
        
        .link.highlight {
            stroke: #e94560;
            stroke-opacity: 1;
            stroke-width: 2px;
        }
        
        .node.highlight circle {
            stroke: #e94560;
            stroke-width: 3px;
        }
        
        /* Node type colors */
        .node-workspace circle { fill: #4a90d9; }
        .node-semantic_model circle { fill: #50c878; }
        .node-report circle { fill: #e94560; }
        .node-notebook circle { fill: #ff6b6b; }
        .node-pipeline circle { fill: #ffd93d; }
        .node-lakehouse circle { fill: #6bcb77; }
        .node-measure circle { fill: #4d96ff; }
        .node-dataflow circle { fill: #9b59b6; }
        .node-unknown circle { fill: #888; }
        
        #legend {
            position: absolute;
            bottom: 20px;
            left: 20px;
            background: rgba(22, 33, 62, 0.95);
            padding: 15px;
            border-radius: 8px;
            border: 1px solid #0f3460;
        }
        
        #legend h3 {
            font-size: 12px;
            color: #888;
            margin-bottom: 10px;
        }
        
        #legend .legend-item {
            display: flex;
            align-items: center;
            margin-bottom: 6px;
            font-size: 12px;
        }
        
        #legend .legend-item .dot {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
        }
        
        .tooltip {
            position: absolute;
            background: rgba(22, 33, 62, 0.95);
            border: 1px solid #0f3460;
            border-radius: 4px;
            padding: 10px;
            font-size: 12px;
            pointer-events: none;
            z-index: 1001;
            max-width: 300px;
        }
        
        .tooltip .title {
            font-weight: 600;
            color: #e94560;
            margin-bottom: 5px;
        }
        
        .tooltip .type {
            color: #888;
            font-size: 11px;
        }
    </style>
</head>
<body>
    <div id="header">
        <h1>Fabric Workspace Dependency Graph</h1>
        <div class="stats">
            <span id="stats-nodes">Nodes: --</span>
            <span id="stats-edges">Edges: --</span>
            <span id="stats-time">Generated: --</span>
        </div>
        <button onclick="resetZoom()">Reset View</button>
        <button onclick="location.reload()">Refresh</button>
    </div>
    
    <div id="graph-container">
        <svg id="graph"></svg>
        
        <div id="legend">
            <h3>Node Types</h3>
            <div class="legend-item">
                <div class="dot" style="background: #4a90d9"></div>
                <span>Workspace</span>
            </div>
            <div class="legend-item">
                <div class="dot" style="background: #50c878"></div>
                <span>Semantic Model</span>
            </div>
            <div class="legend-item">
                <div class="dot" style="background: #e94560"></div>
                <span>Report</span>
            </div>
            <div class="legend-item">
                <div class="dot" style="background: #ff6b6b"></div>
                <span>Notebook</span>
            </div>
            <div class="legend-item">
                <div class="dot" style="background: #ffd93d"></div>
                <span>Pipeline</span>
            </div>
            <div class="legend-item">
                <div class="dot" style="background: #6bcb77"></div>
                <span>Lakehouse</span>
            </div>
            <div class="legend-item">
                <div class="dot" style="background: #4d96ff"></div>
                <span>Measure</span>
            </div>
        </div>
    </div>
    
    <div id="sidebar">
        <div class="section">
            <h2>Selected Node</h2>
            <div id="selected-info">
                <div class="item">
                    <div class="value" style="color: #888">Click a node to see details</div>
                </div>
            </div>
        </div>
        
        <div class="section">
            <h2>Dependencies</h2>
            <div id="dependencies-list">
                <div class="item">
                    <div class="value" style="color: #888">Select a node</div>
                </div>
            </div>
        </div>
        
        <div class="section">
            <h2>Dependents</h2>
            <div id="dependents-list">
                <div class="item">
                    <div class="value" style="color: #888">Select a node</div>
                </div>
            </div>
        </div>
    </div>
    
    <div id="tooltip" class="tooltip" style="display: none"></div>
    
    <script>
        // Graph data (will be replaced)
        const graphData = __GRAPH_DATA__;
        
        // Update stats
        document.getElementById('stats-nodes').textContent = `Nodes: ${graphData.nodes.length}`;
        document.getElementById('stats-edges').textContent = `Edges: ${graphData.edges.length}`;
        document.getElementById('stats-time').textContent = `Generated: ${new Date(graphData.generated_at).toLocaleString()}`;
        
        // Set up SVG
        const container = document.getElementById('graph-container');
        const svg = d3.select('#graph');
        const width = container.clientWidth;
        const height = container.clientHeight;
        
        // Create zoom behavior
        const zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', (event) => {
                g.attr('transform', event.transform);
            });
        
        svg.call(zoom);
        
        const g = svg.append('g');
        
        // Create arrow marker for edges
        svg.append('defs').append('marker')
            .attr('id', 'arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 20)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#0f3460');
        
        // Create force simulation
        const simulation = d3.forceSimulation(graphData.nodes)
            .force('link', d3.forceLink(graphData.edges)
                .id(d => d.id)
                .distance(100))
            .force('charge', d3.forceManyBody().strength(-300))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(30));
        
        // Create links
        const link = g.append('g')
            .attr('class', 'links')
            .selectAll('path')
            .data(graphData.edges)
            .enter()
            .append('path')
            .attr('class', 'link')
            .attr('marker-end', 'url(#arrow)');
        
        // Create nodes
        const node = g.append('g')
            .attr('class', 'nodes')
            .selectAll('g')
            .data(graphData.nodes)
            .enter()
            .append('g')
            .attr('class', d => `node node-${d.type}`)
            .call(d3.drag()
                .on('start', dragstarted)
                .on('drag', dragged)
                .on('end', dragended));
        
        // Node circles
        node.append('circle')
            .attr('r', d => {
                if (d.type === 'workspace') return 20;
                if (d.type === 'semantic_model' || d.type === 'report') return 12;
                if (d.type === 'measure') return 6;
                return 10;
            });
        
        // Node labels
        node.append('text')
            .attr('dx', 15)
            .attr('dy', 4)
            .text(d => d.name.length > 20 ? d.name.substring(0, 20) + '...' : d.name);
        
        // Node interactions
        node.on('click', (event, d) => {
            selectNode(d);
        });
        
        node.on('mouseover', (event, d) => {
            showTooltip(event, d);
        });
        
        node.on('mouseout', () => {
            hideTooltip();
        });
        
        // Update positions on tick
        simulation.on('tick', () => {
            link.attr('d', d => {
                const dx = d.target.x - d.source.x;
                const dy = d.target.y - d.source.y;
                return `M${d.source.x},${d.source.y}L${d.target.x},${d.target.y}`;
            });
            
            node.attr('transform', d => `translate(${d.x},${d.y})`);
        });
        
        // Drag functions
        function dragstarted(event) {
            if (!event.active) simulation.alphaTarget(0.3).restart();
            event.subject.fx = event.subject.x;
            event.subject.fy = event.subject.y;
        }
        
        function dragged(event) {
            event.subject.fx = event.x;
            event.subject.fy = event.y;
        }
        
        function dragended(event) {
            if (!event.active) simulation.alphaTarget(0);
            event.subject.fx = null;
            event.subject.fy = null;
        }
        
        // Tooltip
        function showTooltip(event, d) {
            const tooltip = document.getElementById('tooltip');
            tooltip.innerHTML = `
                <div class="title">${d.name}</div>
                <div class="type">${d.type}</div>
            `;
            tooltip.style.display = 'block';
            tooltip.style.left = (event.pageX + 10) + 'px';
            tooltip.style.top = (event.pageY + 10) + 'px';
        }
        
        function hideTooltip() {
            document.getElementById('tooltip').style.display = 'none';
        }
        
        // Node selection
        let selectedNode = null;
        
        function selectNode(d) {
            // Clear previous selection
            d3.selectAll('.node').classed('highlight', false);
            d3.selectAll('.link').classed('highlight', false);
            
            selectedNode = d;
            
            // Highlight selected node
            d3.selectAll('.node')
                .filter(n => n.id === d.id)
                .classed('highlight', true);
            
            // Find and highlight connected edges
            const connectedLinks = graphData.edges.filter(
                e => e.source.id === d.id || e.target.id === d.id
            );
            
            d3.selectAll('.link')
                .filter(l => connectedLinks.includes(l))
                .classed('highlight', true);
            
            // Update sidebar
            updateSidebar(d);
        }
        
        function updateSidebar(d) {
            // Selected info
            document.getElementById('selected-info').innerHTML = `
                <div class="item">
                    <div class="label">Name</div>
                    <div class="value">${d.name}</div>
                </div>
                <div class="item">
                    <div class="label">Type</div>
                    <div class="value">${d.type}</div>
                </div>
                <div class="item">
                    <div class="label">ID</div>
                    <div class="value" style="font-size: 10px; word-break: break-all">${d.id}</div>
                </div>
            `;
            
            // Find dependencies (edges where this node is source)
            const deps = graphData.edges
                .filter(e => e.source.id === d.id)
                .map(e => graphData.nodes.find(n => n.id === e.target.id))
                .filter(n => n);
            
            if (deps.length > 0) {
                document.getElementById('dependencies-list').innerHTML = deps.map(dep => `
                    <div class="item" onclick="selectNodeById('${dep.id}')" style="cursor: pointer">
                        <div class="label">${dep.type}</div>
                        <div class="value">${dep.name}</div>
                    </div>
                `).join('');
            } else {
                document.getElementById('dependencies-list').innerHTML = `
                    <div class="item">
                        <div class="value" style="color: #888">No dependencies</div>
                    </div>
                `;
            }
            
            // Find dependents (edges where this node is target)
            const dependents = graphData.edges
                .filter(e => e.target.id === d.id)
                .map(e => graphData.nodes.find(n => n.id === e.source.id))
                .filter(n => n);
            
            if (dependents.length > 0) {
                document.getElementById('dependents-list').innerHTML = dependents.map(dep => `
                    <div class="item" onclick="selectNodeById('${dep.id}')" style="cursor: pointer">
                        <div class="label">${dep.type}</div>
                        <div class="value">${dep.name}</div>
                    </div>
                `).join('');
            } else {
                document.getElementById('dependents-list').innerHTML = `
                    <div class="item">
                        <div class="value" style="color: #888">No dependents</div>
                    </div>
                `;
            }
        }
        
        function selectNodeById(id) {
            const node = graphData.nodes.find(n => n.id === id);
            if (node) selectNode(node);
        }
        
        function resetZoom() {
            svg.transition().duration(750).call(
                zoom.transform,
                d3.zoomIdentity.translate(width / 2, height / 2).scale(1)
            );
        }
        
        // Initial zoom to fit
        setTimeout(() => {
            const bounds = g.node().getBBox();
            const dx = bounds.width;
            const dy = bounds.height;
            const x = bounds.x + dx / 2;
            const y = bounds.y + dy / 2;
            const scale = 0.8 / Math.max(dx / width, dy / height);
            const translate = [width / 2 - scale * x, height / 2 - scale * y];
            
            svg.transition().duration(750).call(
                zoom.transform,
                d3.zoomIdentity.translate(translate[0], translate[1]).scale(scale)
            );
        }, 500);
    </script>
</body>
</html>
"""


def generate_viewer_html(graph: Dict[str, Any]) -> str:
    """
    Generate the HTML viewer with embedded graph data.
    
    Args:
        graph: Graph data from WorkspaceGraphBuilder.
    
    Returns:
        Complete HTML string.
    """
    graph_json = json.dumps(graph, indent=2)
    return VIEWER_HTML.replace("__GRAPH_DATA__", graph_json)


def launch_viewer(
    graph: Dict[str, Any],
    port: int = 8765,
    open_browser: bool = True,
) -> None:
    """
    Launch a local HTTP server to view the graph.
    
    Args:
        graph: Graph data from WorkspaceGraphBuilder.
        port: Port for the HTTP server.
        open_browser: Whether to open the browser automatically.
    """
    html_content = generate_viewer_html(graph)
    
    # Create temporary directory and HTML file
    temp_dir = tempfile.mkdtemp()
    html_path = Path(temp_dir) / "index.html"
    html_path.write_text(html_content, encoding="utf-8")
    
    # Create simple HTTP server
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=temp_dir, **kwargs)
        
        def log_message(self, format, *args):
            pass  # Suppress logging
    
    server = HTTPServer(("localhost", port), Handler)
    
    logger.info(f"Starting viewer at http://localhost:{port}")
    
    if open_browser:
        webbrowser.open(f"http://localhost:{port}")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()


async def build_and_view(
    workspace: Optional[str] = None,
    workspace_id: Optional[str] = None,
    graph_path: Optional[str] = None,
    port: int = 8765,
    open_browser: bool = True,
) -> None:
    """
    Build the graph (if needed) and launch the viewer.
    
    Args:
        workspace: Workspace name.
        workspace_id: Workspace ID.
        graph_path: Path to existing graph JSON.
        port: Port for HTTP server.
        open_browser: Whether to open browser.
    """
    if graph_path:
        # Load existing graph
        graph = json.loads(Path(graph_path).read_text(encoding="utf-8"))
    else:
        # Build new graph
        from fabric_agent.core.config import FabricAuthConfig
        from fabric_agent.api.fabric_client import FabricApiClient
        from fabric_agent.tools.workspace_graph import WorkspaceGraphBuilder
        
        cfg = FabricAuthConfig.from_env()
        
        async with FabricApiClient(cfg) as client:
            if not workspace_id and workspace:
                # Resolve workspace name to ID
                resp = await client.get("/workspaces")
                values = resp.get("value", [])
                for w in values:
                    if str(w.get("displayName", "")).lower() == workspace.lower():
                        workspace_id = str(w.get("id"))
                        break
                
                if not workspace_id:
                    raise ValueError(f"Workspace not found: {workspace}")
            
            if not workspace_id:
                raise ValueError("Provide --workspace or --workspace-id")
            
            builder = WorkspaceGraphBuilder()
            graph = await builder.build(client, workspace_id, workspace)
    
    # Launch viewer
    launch_viewer(graph, port=port, open_browser=open_browser)


def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""
    p = argparse.ArgumentParser(description="View Fabric workspace dependency graph")
    p.add_argument("--graph", help="Path to existing graph JSON file")
    p.add_argument("--workspace", help="Workspace display name (builds graph live)")
    p.add_argument("--workspace-id", help="Workspace UUID")
    p.add_argument("--port", type=int, default=8765, help="HTTP server port")
    p.add_argument("--open", action="store_true", help="Open browser automatically")
    return p


def main() -> None:
    """CLI entry point."""
    args = build_parser().parse_args()
    
    if not args.graph and not args.workspace and not args.workspace_id:
        print("Error: Provide --graph (existing file) or --workspace/--workspace-id (build live)")
        return
    
    asyncio.run(build_and_view(
        workspace=args.workspace,
        workspace_id=args.workspace_id,
        graph_path=args.graph,
        port=args.port,
        open_browser=args.open,
    ))


if __name__ == "__main__":
    main()
