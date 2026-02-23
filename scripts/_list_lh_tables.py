"""Quick script to list lakehouses and their registered Delta tables."""
import asyncio, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=True)

from fabric_agent.core.config import AgentConfig
from fabric_agent.api.fabric_client import FabricApiClient

DEV_WS = "0359f4ba-9cd9-4652-8438-3b77368a3cb7"

async def main():
    config = AgentConfig()
    config.load_auth_from_env()
    client = FabricApiClient(auth_config=config.auth)
    await client.initialize()

    data = await client.get(f"/workspaces/{DEV_WS}/items", params={"type": "Lakehouse"})
    for lh in data.get("value", []):
        name = lh["displayName"]
        lh_id = lh["id"]
        print(f"\n{name}  ({lh_id})")

        # List shortcuts
        sc = await client.get(f"/workspaces/{DEV_WS}/items/{lh_id}/shortcuts")
        shortcuts = sc.get("value", [])
        print(f"  shortcuts: {[s.get('name') for s in shortcuts]}")

        # List Delta tables
        try:
            tables = await client.get(f"/workspaces/{DEV_WS}/lakehouses/{lh_id}/tables")
            tbl_names = [t.get("name") for t in tables.get("data", [])]
            print(f"  delta_tables: {tbl_names}")
        except Exception as exc:
            print(f"  delta_tables: (error: {exc})")

    await client.close()

asyncio.run(main())
