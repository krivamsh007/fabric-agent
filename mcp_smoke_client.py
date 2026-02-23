"""MCP stdio smoke client for this repo.

Runs the MCP server as a subprocess, performs the MCP initialize handshake,
then lists tools and calls ``list_workspaces`` if available.

Usage (from repo root):
    python mcp_smoke_client.py
"""

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv


async def main() -> None:
    # Load .env so the spawned server inherits Fabric service principal vars
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env", override=True)

    # MCP client imports (mcp>=1.2x)
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)

    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "src.mcp.server"],
        env=env,
    )

    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = await session.list_tools()
            tool_names = [t.name for t in tools.tools]
            print("TOOLS:", tool_names)

            if "list_workspaces" in tool_names:
                res = await session.call_tool("list_workspaces", {})
                print("list_workspaces:", res)
            else:
                print("No tool named 'list_workspaces'. Use TOOLS list to call the right name.")


if __name__ == "__main__":
    asyncio.run(main())
