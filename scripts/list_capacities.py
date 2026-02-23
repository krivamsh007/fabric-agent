import asyncio
from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.api.fabric_client import FabricApiClient

async def main():
    cfg = FabricAuthConfig.from_env()
    async with FabricApiClient(cfg) as client:
        caps = await client.get("/capacities")
        for c in caps.get("value", []):
            print(f"{c.get('displayName')}  ->  {c.get('id')}")

asyncio.run(main())
