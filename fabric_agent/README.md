# 📦 fabric_agent Package - Architecture Documentation

## Overview

The `fabric_agent` package is the core library for the Microsoft Fabric Refactoring Agent. It provides:

- Multi-agent AI system for safe refactoring
- Fabric API client with OAuth2 authentication
- MCP (Model Context Protocol) server for AI integration
- Workspace dependency graph analysis
- Checkpoint/rollback system

---

# 📁 Package Structure

```
fabric_agent/                    # 15,508 lines total
├── __init__.py                 (38 lines)   - Package exports
├── mcp_server.py               (879 lines)  - MCP server for Claude/GPT
├── cli.py                      (283 lines)  - CLI entry points
│
├── agents/                     # Multi-Agent System (2,537 lines)
│   ├── __init__.py             (102 lines)  - Agent exports
│   ├── base.py                 (603 lines)  - BaseAgent, SharedMemory, LLM clients
│   ├── specialized.py          (574 lines)  - Discovery, Impact, Refactor agents
│   ├── validation.py           (521 lines)  - ValidationAgent
│   └── orchestrator.py         (737 lines)  - Workflow orchestration
│
├── api/                        # Fabric API Client (570 lines)
│   ├── __init__.py             (22 lines)   - API exports
│   └── fabric_client.py        (548 lines)  - Async Fabric API client
│
├── core/                       # Core Configuration (971 lines)
│   ├── __init__.py             (22 lines)   - Core exports
│   ├── config.py               (266 lines)  - Auth & app config
│   └── agent.py                (683 lines)  - FabricAgent main class
│
├── refactor/                   # Refactoring Engine (1,457 lines)
│   ├── __init__.py             (30 lines)   - Refactor exports
│   ├── executor.py             (708 lines)  - Actual refactoring execution
│   └── orchestrator.py         (719 lines)  - Refactor workflow
│
├── storage/                    # Persistence Layer (1,053 lines)
│   ├── __init__.py             (26 lines)   - Storage exports
│   └── memory_manager.py       (1,027 lines) - Audit trail, checkpoints
│
├── tools/                      # MCP Tools & Utilities (5,936 lines)
│   ├── __init__.py             (186 lines)  - Tool exports
│   ├── models.py               (805 lines)  - Pydantic models
│   ├── fabric_tools.py         (1,348 lines) - Fabric operation tools
│   ├── workspace_graph.py      (930 lines)  - Dependency graph builder
│   ├── safety_refactor.py      (1,333 lines) - Safe refactoring logic
│   ├── enterprise_dax_generator.py (413 lines) - DAX measure generation
│   ├── enterprise_data_generator.py (901 lines) - Test data generation
│   └── setup_enterprise_demo.py (923 lines) - Demo setup
│
├── ui/                         # Visualization (744 lines)
│   ├── __init__.py             (18 lines)   - UI exports
│   └── graph_viewer.py         (726 lines)  - Browser-based graph viewer
│
└── utils/                      # Utilities (137 lines)
    └── __init__.py             (137 lines)  - Helper functions
```

---

# 🤖 agents/ - Multi-Agent System

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AGENT ORCHESTRATOR                           │
│                     (orchestrator.py - 737 lines)                   │
│                                                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌───────────┐  │
│  │  Discovery  │  │   Impact    │  │  Refactor   │  │ Validation│  │
│  │    Agent    │──│    Agent    │──│    Agent    │──│   Agent   │  │
│  │ (574 lines) │  │ (574 lines) │  │ (574 lines) │  │(521 lines)│  │
│  └─────────────┘  └─────────────┘  └─────────────┘  └───────────┘  │
│         │                │                │               │         │
│         └────────────────┴────────────────┴───────────────┘         │
│                                 │                                   │
│                    ┌────────────▼────────────┐                      │
│                    │     SHARED MEMORY       │                      │
│                    │    (base.py - 603)      │                      │
│                    └─────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Classes

### base.py (603 lines)

| Class | Purpose |
|-------|---------|
| `AgentRole` | Enum: DISCOVERY, IMPACT, REFACTOR, VALIDATION, ORCHESTRATOR |
| `MessageType` | Enum: REQUEST, RESPONSE, NOTIFICATION, ERROR |
| `TaskStatus` | Enum: PENDING, RUNNING, COMPLETED, FAILED, CANCELLED |
| `AgentMessage` | Message passed between agents |
| `ToolDefinition` | Tool definition for LLM |
| `ToolCall` | Record of a tool call |
| `AgentThought` | Reasoning step from agent |
| `AgentResult` | Result of agent execution |
| `SharedMemory` | Shared state accessible by all agents |
| `LLMClient` | Abstract interface for LLM providers |
| `ClaudeLLMClient` | Claude (Anthropic) implementation |
| `BaseAgent` | Base class for all agents |
| `SimpleAgent` | Agent without LLM reasoning |

### specialized.py (574 lines)

| Agent | Responsibilities |
|-------|------------------|
| `DiscoveryAgent` | Scan workspaces, build dependency graphs, detect changes |
| `ImpactAgent` | Analyze changes, score risk, identify affected items |
| `RefactorAgent` | Execute changes with safety checks, update references |

### validation.py (521 lines)

| Class | Purpose |
|-------|---------|
| `ValidationAgent` | Test DAX queries, verify reports, check syntax |
| `ValidationResult` | Result of validation checks |
| `ValidationSuite` | Collection of validation tests |

### orchestrator.py (737 lines)

| Class | Purpose |
|-------|---------|
| `WorkflowStatus` | PENDING, DISCOVERING, ANALYZING, AWAITING_APPROVAL, etc. |
| `ApprovalRequest` | Human approval request with risk level |
| `WorkflowResult` | Complete workflow execution result |
| `DeploymentPipelineManager` | Integration with Fabric Deployment Pipelines |
| `AgentOrchestrator` | Coordinates multi-agent workflows |

---

# 🌐 api/ - Fabric API Client

## fabric_client.py (548 lines)

### Key Features

- **OAuth2 Authentication**: Service principal or interactive
- **Rate Limiting**: Exponential backoff with jitter
- **LRO Handling**: Long-running operation polling
- **Retry Logic**: Automatic retry on transient failures
- **Async Context Manager**: Proper resource cleanup

### Usage

```python
from fabric_agent.api.fabric_client import FabricApiClient
from fabric_agent.core.config import FabricAuthConfig

config = FabricAuthConfig.from_env()

async with FabricApiClient(config) as client:
    # List workspaces
    workspaces = await client.get("/workspaces")
    
    # Create lakehouse (with LRO)
    result = await client.post_with_lro(
        f"/workspaces/{ws_id}/lakehouses",
        json_data={"displayName": "My_Lakehouse"},
        lro_poll_seconds=3.0,
        max_polls=60,
    )
```

### Methods

| Method | Purpose |
|--------|---------|
| `get(path)` | GET request |
| `post(path, json_data)` | POST request |
| `patch(path, json_data)` | PATCH request |
| `delete(path)` | DELETE request |
| `post_with_lro(path, json_data)` | POST with long-running operation handling |
| `get_raw(path)` | Raw httpx response |

---

# 🔧 tools/ - MCP Tools & Utilities

## workspace_graph.py (930 lines)

### Purpose
Builds comprehensive dependency graphs for Fabric workspaces.

### Key Classes

| Class | Purpose |
|-------|---------|
| `NodeType` | WORKSPACE, SEMANTIC_MODEL, REPORT, NOTEBOOK, etc. |
| `EdgeType` | USES_MODEL, REFERENCES_MEASURE, EXECUTES, etc. |
| `GraphNode` | Node in the dependency graph |
| `GraphEdge` | Edge (dependency) between nodes |
| `WorkspaceGraph` | Complete graph structure |
| `WorkspaceGraphBuilder` | Builds graphs from Fabric API |

### DAX Parsing

```python
def _parse_measure_references(self, expression: str) -> Set[str]:
    """Extract measure references from DAX expression."""
    # Finds [MeasureName] patterns
    pattern = r'\[([^\]]+)\]'
    matches = re.findall(pattern, expression)
    return set(matches)
```

## safety_refactor.py (1,333 lines)

### Purpose
Safe refactoring with checkpoints and rollback.

### Key Classes

| Class | Purpose |
|-------|---------|
| `RefactorPlan` | Planned changes before execution |
| `RefactorCheckpoint` | Saved state for rollback |
| `SafeRefactorEngine` | Main refactoring engine |

### Workflow

```python
engine = SafeRefactorEngine(client, workspace_id, model_id)

# 1. Plan
plan = await engine.plan_rename("Old Name", "New Name")

# 2. Create checkpoint
checkpoint_id = await engine.create_checkpoint()

# 3. Execute
result = await engine.execute(plan)

# 4. Validate
is_valid = await engine.validate()

# 5. Rollback if needed
if not is_valid:
    await engine.rollback(checkpoint_id)
```

## fabric_tools.py (1,348 lines)

### Purpose
MCP tool implementations for Fabric operations.

### Tools Provided

| Tool | Description |
|------|-------------|
| `list_workspaces` | List accessible workspaces |
| `set_workspace` | Set active workspace |
| `list_items` | List items in workspace |
| `get_semantic_model` | Get model details |
| `get_measures` | Get DAX measures |
| `analyze_impact` | Analyze change impact |
| `rename_measure` | Rename with safety |
| `rollback` | Rollback to checkpoint |

---

# 💾 storage/ - Persistence Layer

## memory_manager.py (1,027 lines)

### Purpose
Persistent storage for audit trail, checkpoints, and state.

### Key Classes

| Class | Purpose |
|-------|---------|
| `OperationLog` | Single operation record |
| `Checkpoint` | Saved state for rollback |
| `MemoryManager` | SQLite-backed persistence |

### Storage Schema

```sql
-- Operations table
CREATE TABLE operations (
    id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    workspace_id TEXT,
    model_id TEXT,
    target_name TEXT,
    old_value TEXT,
    new_value TEXT,
    status TEXT,
    error_message TEXT,
    metadata TEXT  -- JSON
);

-- Checkpoints table
CREATE TABLE checkpoints (
    id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    model_definition TEXT NOT NULL,  -- Full TMSL/TMDL
    metadata TEXT  -- JSON
);
```

---

# 🖥️ mcp_server.py - MCP Integration

## Purpose
Exposes Fabric operations as MCP tools for AI agents.

## Available Tools

| Tool | Input Schema |
|------|--------------|
| `list_workspaces` | `{}` |
| `set_workspace` | `{workspace_name: string}` |
| `list_items` | `{item_type?: string}` |
| `get_semantic_model` | `{model_name: string}` |
| `get_measures` | `{model_name: string}` |
| `analyze_impact` | `{target_type, target_name, change_type}` |
| `rename_measure` | `{model_name, old_name, new_name, dry_run?}` |
| `smart_rename_measure` | `{model_name, old_name, new_name}` |
| `rollback` | `{operation_id}` |
| `get_refactor_history` | `{limit?: number}` |

## Usage with Claude

```json
// mcp_config.json
{
  "mcpServers": {
    "fabric-agent": {
      "command": "python",
      "args": ["-m", "fabric_agent.mcp_server"],
      "env": {
        "AZURE_TENANT_ID": "...",
        "AZURE_CLIENT_ID": "...",
        "AZURE_CLIENT_SECRET": "..."
      }
    }
  }
}
```

---

# 🎨 ui/ - Visualization

## graph_viewer.py (726 lines)

### Purpose
Browser-based dependency graph visualization.

### Usage

```bash
# Start viewer
python -m fabric_agent.ui.graph_viewer \
    --graph memory/workspace_graph.json \
    --open

# With live refresh from Fabric
python -m fabric_agent.ui.graph_viewer \
    --graph memory/workspace_graph.json \
    --workspace "my-workspace" \
    --open
```

### Features

- Interactive D3.js visualization
- Node filtering by type
- Edge highlighting
- Search functionality
- Live refresh from Fabric

---

# 📊 Module Dependency Graph

```
mcp_server.py
    ├── core/agent.py
    │   ├── core/config.py
    │   ├── api/fabric_client.py
    │   └── tools/fabric_tools.py
    │       ├── tools/models.py
    │       ├── tools/workspace_graph.py
    │       └── tools/safety_refactor.py
    │           └── storage/memory_manager.py
    │
    └── agents/orchestrator.py
        ├── agents/base.py
        ├── agents/specialized.py
        │   └── tools/workspace_graph.py
        └── agents/validation.py
```

---

# 🧪 Testing

## Test Locations

| Test | Location |
|------|----------|
| Memory Manager | `tests/test_memory_manager.py` |
| Models | `tests/test_models.py` |
| Workspace Graph | `tests/test_workspace_graph.py` |

## Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test
python -m pytest tests/test_memory_manager.py -v

# Run with coverage
python -m pytest tests/ --cov=fabric_agent
```

---

# 📈 Quality Metrics

| Metric | Value |
|--------|-------|
| Total Lines | 15,508 |
| Python Files | 24 |
| Type Hints | ~85% coverage |
| Docstrings | ~75% coverage |
| Test Coverage | ~45% (needs improvement) |
