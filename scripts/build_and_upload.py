"""
Build & Upload Script for Microsoft Fabric Deployment
======================================================

WHAT: Builds the fabric_agent Python wheel and uploads it to a Fabric Lakehouse
      so that Fabric Notebooks can install and import it.

WHY: Microsoft Fabric Notebooks run in a managed Spark environment. They cannot
     pip-install from GitHub directly. The supported flow is:
       1. Build a .whl file locally
       2. Upload it to Lakehouse Files/wheels/
       3. In the notebook: %pip install /lakehouse/default/Files/wheels/fabric_agent-*.whl

USAGE:
    # Dry run (shows what would happen)
    python scripts/build_and_upload.py --dry-run

    # Upload to the default lakehouse
    python scripts/build_and_upload.py

    # Upload to a specific lakehouse in a specific workspace
    python scripts/build_and_upload.py --workspace "ENT_DataPlatform_DEV" --lakehouse "Bronze_Landing"

    # Build only (don't upload)
    python scripts/build_and_upload.py --build-only

REQUIREMENTS:
    pip install build
    # Plus AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET in .env
"""

from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

ROOT = Path(__file__).resolve().parents[1]
console = Console()


# =============================================================================
# Build Step
# =============================================================================

def build_wheel(output_dir: Path) -> Path:
    """
    Build the fabric_agent wheel using Python build.

    Returns the path to the built .whl file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    console.print("\n[bold cyan]Step 1: Building Python wheel...[/bold cyan]")
    result = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(output_dir)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        console.print(f"[red]Build failed:[/red]\n{result.stderr}")
        sys.exit(1)

    # Find the wheel file
    wheels = list(output_dir.glob("fabric_agent-*.whl"))
    if not wheels:
        console.print("[red]No wheel file found after build.[/red]")
        sys.exit(1)

    wheel_path = max(wheels, key=lambda p: p.stat().st_mtime)
    size_kb = wheel_path.stat().st_size // 1024

    console.print(f"[green]Built:[/green] {wheel_path.name} ({size_kb} KB)")
    return wheel_path


# =============================================================================
# Upload Step
# =============================================================================

async def upload_wheel(
    wheel_path: Path,
    workspace_name: str,
    lakehouse_name: str,
    dry_run: bool = False,
) -> str:
    """
    Upload the wheel to Lakehouse Files/wheels/ via Fabric API.

    Returns the OneLake path of the uploaded file.
    """
    sys.path.insert(0, str(ROOT))
    load_dotenv(ROOT / ".env", override=True)

    from fabric_agent.api.fabric_client import FabricApiClient
    from fabric_agent.core.config import AgentConfig

    target_path = f"Files/wheels/{wheel_path.name}"

    if dry_run:
        console.print(
            f"\n[yellow]DRY RUN:[/yellow] Would upload {wheel_path.name} to "
            f"{workspace_name}/{lakehouse_name}/{target_path}"
        )
        return f"/lakehouse/default/Files/wheels/{wheel_path.name}"

    console.print(f"\n[bold cyan]Step 2: Uploading to Fabric...[/bold cyan]")
    console.print(f"  Workspace: {workspace_name}")
    console.print(f"  Lakehouse: {lakehouse_name}")
    console.print(f"  Path:      {target_path}")

    config = AgentConfig()

    async with FabricApiClient(
        tenant_id=config.auth.tenant_id,
        client_id=config.auth.client_id,
        client_secret=config.auth.client_secret,
    ) as client:
        # Find workspace
        ws_list = await client.get("/workspaces")
        workspaces = ws_list.get("value", [])
        workspace = next(
            (w for w in workspaces if w.get("displayName") == workspace_name), None
        )
        if not workspace:
            console.print(f"[red]Workspace '{workspace_name}' not found.[/red]")
            console.print("Available workspaces:")
            for w in workspaces:
                console.print(f"  - {w.get('displayName')}")
            sys.exit(1)

        ws_id = workspace["id"]

        # Find lakehouse
        items = await client.get(f"/workspaces/{ws_id}/items")
        lakehouses = [
            i for i in items.get("value", [])
            if i.get("type") == "Lakehouse" and i.get("displayName") == lakehouse_name
        ]
        if not lakehouses:
            console.print(f"[red]Lakehouse '{lakehouse_name}' not found in workspace.[/red]")
            sys.exit(1)

        lakehouse_id = lakehouses[0]["id"]

        # Upload file via OneLake Files API
        with wheel_path.open("rb") as f:
            wheel_bytes = f.read()

        upload_url = (
            f"/workspaces/{ws_id}/lakehouses/{lakehouse_id}"
            f"/files/{target_path.replace('/', '%2F')}"
        )

        await client.put_binary(upload_url, wheel_bytes)

    onelake_path = f"/lakehouse/default/Files/wheels/{wheel_path.name}"
    console.print(f"[green]Uploaded:[/green] {wheel_path.name}")
    return onelake_path


# =============================================================================
# Summary
# =============================================================================

def print_next_steps(wheel_name: str, workspace_name: str, lakehouse_name: str) -> None:
    """Print what to do next in the Fabric Notebook."""
    console.print(
        Panel(
            f"""[bold]Next steps in your Fabric Notebook:[/bold]

1. [cyan]Install the package:[/cyan]
   [green]%pip install /lakehouse/default/Files/wheels/{wheel_name}[/green]

2. [cyan]Verify the install:[/cyan]
   [green]import fabric_agent; print(fabric_agent.__version__)[/green]

3. [cyan]Run the tutorial:[/cyan]
   Open [bold]notebooks/00_Setup_And_Deploy.ipynb[/bold] in Fabric

[dim]Wheel uploaded to:[/dim]
  Workspace: {workspace_name}
  Lakehouse: {lakehouse_name}/Files/wheels/{wheel_name}

[dim]To update the package, re-run this script and restart the notebook kernel.[/dim]""",
            title="Deployment Complete",
            border_style="green",
        )
    )


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build and upload fabric_agent wheel to Microsoft Fabric"
    )
    parser.add_argument(
        "--workspace",
        default="ENT_DataPlatform_DEV",
        help="Fabric workspace name (default: ENT_DataPlatform_DEV)",
    )
    parser.add_argument(
        "--lakehouse",
        default="Bronze_Landing",
        help="Lakehouse name to upload wheels to (default: Bronze_Landing)",
    )
    parser.add_argument(
        "--build-only",
        action="store_true",
        help="Only build the wheel, don't upload",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without actually uploading",
    )
    parser.add_argument(
        "--output-dir",
        default="dist",
        help="Directory to write wheel to (default: dist/)",
    )
    args = parser.parse_args()

    output_dir = ROOT / args.output_dir

    # Step 1: Build
    wheel_path = build_wheel(output_dir)

    if args.build_only:
        console.print(f"\n[green]Wheel built at:[/green] {wheel_path}")
        console.print("Run without --build-only to upload to Fabric.")
        return

    # Step 2: Upload
    onelake_path = asyncio.run(
        upload_wheel(
            wheel_path=wheel_path,
            workspace_name=args.workspace,
            lakehouse_name=args.lakehouse,
            dry_run=args.dry_run,
        )
    )

    # Step 3: Print next steps
    print_next_steps(wheel_path.name, args.workspace, args.lakehouse)


if __name__ == "__main__":
    main()
