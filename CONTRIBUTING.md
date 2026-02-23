# Contributing to Fabric Agent

Thank you for your interest in contributing. This document explains how to set up
a local development environment, understand the codebase conventions, and submit a change.

---

## Table of Contents

- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Adding a New MCP Tool](#adding-a-new-mcp-tool)
- [Adding a New Agent](#adding-a-new-agent)
- [Testing](#testing)
- [Code Style](#code-style)
- [Pull Request Checklist](#pull-request-checklist)

---

## Development Setup

### 1. Fork and clone

```bash
git clone https://github.com/<your-github-username>/fabric-agent.git
cd fabric-agent
```

### 2. Create a virtual environment

```bash
python -m venv .venv

# Windows PowerShell
.\.venv\Scripts\Activate.ps1

# macOS / Linux
source .venv/bin/activate
```

### 3. Install in editable mode with dev extras

```bash
pip install -U pip
pip install -e ".[dev,memory,healing]"
```

The `dev` extra installs pytest, black, ruff, and mypy.
The `memory` and `healing` extras are required for the full test suite.

### 4. Configure credentials (optional — not required for tests)

```bash
# Windows
copy .env.template .env

# macOS / Linux
cp .env.template .env
```

Fill in your Azure credentials only if you want to run scripts against a live Fabric tenant.
All automated tests use mocks and require no credentials.

### 5. Run the test suite to verify setup

```bash
pytest tests/ -v
```

All tests should pass. If any fail on a fresh clone, please open an issue.

---

## Project Structure

The codebase follows a layered architecture. The key rule is:
**higher layers call lower layers, never the reverse.**

```
fabric_agent/
├── agents/          # Specialized agents (Discovery, Impact, Refactor, Healer)
│   ├── base.py      # SimpleAgent base class — all agents extend this
│   └── specialized.py
├── healing/         # Self-healing use case
│   ├── models.py    # Anomaly, HealingPlan, HealthReport dataclasses
│   ├── detector.py  # AnomalyDetector — reads lineage graph, finds anomalies
│   ├── healer.py    # SelfHealer — executes fixes via Fabric API
│   └── monitor.py   # SelfHealingMonitor — orchestrates detect → plan → heal loop
├── memory/          # Vector RAG use case
│   ├── embedding_client.py  # Abstract EmbeddingClient + LocalEmbeddingClient
│   ├── vector_store.py      # Abstract VectorStore + ChromaVectorStore
│   ├── operation_memory.py  # OperationMemory — main class: index + retrieve
│   └── session_context.py   # Cross-session state persistence
├── lineage/         # Dependency graph engine
├── schema/          # Schema drift detection
├── refactor/        # Safe DAX refactoring with rollback
├── storage/         # SQLite audit trail (MemoryManager, StateSnapshot)
├── tools/
│   ├── models.py    # All Pydantic input/output models — these are the API contract
│   └── fabric_tools.py   # FabricTools class — one method per MCP tool
├── core/
│   └── fabric_env.py    # Detects Fabric vs local environment at runtime
├── mcp_server.py    # MCP server entry point — routes tool calls to FabricTools
└── cli.py           # Typer CLI entry point
```

---

## Adding a New MCP Tool

All new tools follow the same three-file pattern. If you skip any step, the tool will
not be discoverable by MCP clients.

### Step 1 — Define Pydantic models in `tools/models.py`

```python
class MyNewToolInput(BaseModel):
    workspace_id: str = Field(..., description="Target workspace GUID")
    dry_run: bool = Field(default=True, description="If true, no changes are made")

class MyNewToolOutput(BaseModel):
    success: bool
    message: str
    items_affected: int
```

Field names in `models.py` are the public contract — do not rename them without a
changelog entry, as MCP clients may be passing these by name.

### Step 2 — Implement the method in `tools/fabric_tools.py`

```python
async def my_new_tool(self, workspace_id: str, dry_run: bool = True) -> MyNewToolOutput:
    """
    One-line description for the MCP tool listing.

    Detailed explanation of what this tool does, any side effects,
    and what the caller should expect back.
    """
    # ... implementation
    return MyNewToolOutput(success=True, message="...", items_affected=0)
```

Log every mutation to the audit trail:

```python
await self._memory_manager.record_state(
    StateType.CUSTOM,
    operation="my_new_tool",
    old_value={"before": ...},
    new_value={"after": ...},
)
```

### Step 3 — Register the route in `mcp_server.py`

```python
@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    # ... existing routes ...
    elif name == "my_new_tool":
        inp = MyNewToolInput(**arguments)
        result = await tools.my_new_tool(inp.workspace_id, inp.dry_run)
        return [types.TextContent(type="text", text=result.model_dump_json(indent=2))]
```

Also register the tool metadata in the `list_tools()` handler so it appears in tool listings.

---

## Adding a New Agent

Agents extend `SimpleAgent` from `agents/base.py` and expose a typed set of tools
to the underlying LLM:

```python
from fabric_agent.agents.base import SimpleAgent, ToolDefinition

class MyNewAgent(SimpleAgent):
    """
    Short description of what this agent does.

    FAANG PARALLEL: (explain what FAANG company runs this type of agent)
    """

    def __init__(self, config: AgentConfig, fabric_tools: FabricTools):
        tools = [
            ToolDefinition(
                name="my_new_tool",
                description="...",
                input_schema=MyNewToolInput.model_json_schema(),
            ),
        ]
        super().__init__(config=config, tools=tools)
        self._fabric_tools = fabric_tools

    async def _execute_tool(self, tool_name: str, tool_input: dict) -> str:
        if tool_name == "my_new_tool":
            result = await self._fabric_tools.my_new_tool(**tool_input)
            return result.model_dump_json()
        raise ValueError(f"Unknown tool: {tool_name}")
```

---

## Testing

### Running tests

```bash
# All tests
pytest tests/ -v

# Specific test files
pytest tests/test_operation_memory.py -v
pytest tests/test_healing.py -v

# With coverage
pytest tests/ --cov=fabric_agent --cov-report=term-missing
```

### Test conventions

- **No real Fabric API calls in tests.** All HTTP calls are mocked with `AsyncMock` or
  `respx`/`httpx` mocking.
- **No real embeddings in tests.** Use `MockEmbeddingClient` which returns deterministic
  fixed-length vectors.
- **No real vector store in tests.** Use `InMemoryVectorStore` from `tests/` which stores
  embeddings in a Python dict.
- Tests are async: decorate with `@pytest.mark.asyncio` or rely on `asyncio_mode = "auto"`
  from `pyproject.toml`.

### Example mock pattern

```python
@pytest.mark.asyncio
async def test_my_new_tool():
    tools = FabricTools.__new__(FabricTools)
    tools._client = AsyncMock()
    tools._memory_manager = AsyncMock()

    result = await tools.my_new_tool(workspace_id="ws-123", dry_run=True)
    assert result.success is True
```

---

## Code Style

This project uses `black` for formatting and `ruff` for linting:

```bash
# Format
black fabric_agent/ tests/

# Lint
ruff check fabric_agent/ tests/

# Type check
mypy fabric_agent/
```

Configuration is in `pyproject.toml`. The CI pipeline runs all three checks.

### Docstring pattern

New classes and public methods should include the following sections where relevant:

```python
class MyClass:
    """
    Short one-line description.

    WHAT: What this class does in plain English.

    WHY: Why this pattern is used (vs simpler alternatives).

    FAANG PARALLEL: What major tech company calls this the same pattern.

    HOW IT WORKS:
    1. Step one
    2. Step two
    3. Step three

    USAGE:
        obj = MyClass(...)
        result = await obj.do_something(...)
    """
```

This docstring pattern is intentional — the project doubles as a learning resource,
and these explanations help engineers understand not just *what* the code does but *why*.

---

## Pull Request Checklist

Before submitting a PR, confirm the following:

- [ ] `pytest tests/ -v` passes with no failures
- [ ] `black fabric_agent/ tests/` produces no changes (code is formatted)
- [ ] `ruff check fabric_agent/ tests/` produces no errors
- [ ] New MCP tools have Pydantic models in `models.py`, implementation in `fabric_tools.py`,
      and a route in `mcp_server.py`
- [ ] All state mutations call `memory_manager.record_state(...)` for the audit trail
- [ ] No real credentials or resource GUIDs in source code (use `<your-...>` placeholders)
- [ ] New public classes and methods have docstrings (see pattern above)
- [ ] PR description explains *what* changed and *why*

---

## Questions

Open a GitHub issue with the `question` label or start a Discussion.
