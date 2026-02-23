#!/usr/bin/env python3
"""
Export Semantic Model Definition to Local File
===============================================

Run this ONCE to export your live Fabric semantic model definition (model.bim)
to  data/templates/semantic_model/model.bim  in this repo.

After exporting, the bootstrap will use that file instead of generating a
synthetic definition from code — so your deployed models are identical to
the production model you captured.

Usage
-----
    python scripts/export_semantic_model.py \\
        --workspace "ENT_SalesAnalytics_DEV" \\
        --model "Enterprise_Sales_Model"

    # Write to a custom path
    python scripts/export_semantic_model.py \\
        --workspace "ENT_SalesAnalytics_DEV" \\
        --model "Enterprise_Sales_Model" \\
        --out data/templates/semantic_model/model.bim

Then commit the exported file:
    git add data/templates/semantic_model/model.bim
    git commit -m "Add exported semantic model definition"

The bootstrap's create_semantic_model() will automatically detect and use this
file on the next run.

FAANG Parallel
--------------
This is the same pattern as "schema-as-code" used at LinkedIn (DataHub),
Airbnb (Minerva), and Google (Spanner schema files).  The authoritative model
definition lives in source control, not as a hidden dependency on a live cloud
resource.  Bootstrap reads from the file; deploy = apply file to cloud.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):  # type: ignore[misc]
        pass

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

sys.path.insert(0, str(ROOT))

from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.api.fabric_client import FabricApiClient

DEFAULT_OUT = ROOT / "data" / "templates" / "semantic_model" / "model.bim"


async def export_model(workspace_name: str, model_name: str, out_path: Path) -> None:
    """Fetch the model.bim definition from Fabric and write it to disk."""
    config = FabricAuthConfig()
    async with FabricApiClient(config) as client:
        # 1) Resolve workspace ID
        ws_response = await client.get("/workspaces")
        workspace_id: str | None = None
        for ws in ws_response.get("value", []):
            if ws.get("displayName") == workspace_name:
                workspace_id = ws["id"]
                break
        if not workspace_id:
            print(f"ERROR: Workspace '{workspace_name}' not found.")
            print("Available workspaces:")
            for ws in ws_response.get("value", []):
                print(f"  {ws.get('displayName')} ({ws.get('id')})")
            sys.exit(1)

        # 2) Resolve semantic model ID
        models_response = await client.get(f"/workspaces/{workspace_id}/semanticModels")
        model_id: str | None = None
        for m in models_response.get("value", []):
            if m.get("displayName") == model_name:
                model_id = m["id"]
                break
        if not model_id:
            print(f"ERROR: Semantic model '{model_name}' not found in workspace '{workspace_name}'.")
            available = [m.get("displayName") for m in models_response.get("value", [])]
            print(f"Available models: {available}")
            sys.exit(1)

        print(f"Found: {model_name} ({model_id}) in {workspace_name} ({workspace_id})")

        # 3) Fetch definition via the getDefinition LRO endpoint
        print("Fetching model definition (this may take a few seconds)...")
        definition_response = await client.post_with_lro(
            f"/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition",
            json_data={},
            lro_poll_seconds=2.0,
            max_polls=60,
        )

        # 4) Find model.bim part in the definition parts
        parts = definition_response.get("definition", {}).get("parts", [])
        model_bim_b64: str | None = None
        for part in parts:
            if part.get("path", "").lower() == "model.bim":
                model_bim_b64 = part.get("payload")
                break

        if not model_bim_b64:
            # Fall back: try first part with InlineBase64
            for part in parts:
                if part.get("payloadType") == "InlineBase64":
                    model_bim_b64 = part.get("payload")
                    print(f"  (used fallback part: {part.get('path')})")
                    break

        if not model_bim_b64:
            print("ERROR: Could not find model.bim in the definition response.")
            print("Definition parts found:")
            for part in parts:
                print(f"  path={part.get('path')}  payloadType={part.get('payloadType')}")
            sys.exit(1)

        # 5) Decode and pretty-print JSON so the file is human-readable and diffable
        raw_bytes = base64.b64decode(model_bim_b64)
        try:
            model_dict = json.loads(raw_bytes.decode("utf-8"))
            pretty_json = json.dumps(model_dict, indent=2, ensure_ascii=False)
            content = pretty_json.encode("utf-8")
        except json.JSONDecodeError:
            # Not JSON (TMDL format) — write raw bytes
            content = raw_bytes

        # 6) Write to disk
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(content)
        size_kb = len(content) / 1024
        print(f"\nExported {size_kb:.1f} KB → {out_path}")
        print("\nNext steps:")
        print("  git add data/templates/semantic_model/model.bim")
        print('  git commit -m "Add exported semantic model definition"')
        print("\nThe bootstrap will now use this file automatically.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a Fabric semantic model definition to data/templates/semantic_model/model.bim"
    )
    parser.add_argument(
        "--workspace",
        required=True,
        help="Display name of the Fabric workspace (e.g. 'ENT_SalesAnalytics_DEV')",
    )
    parser.add_argument(
        "--model",
        required=True,
        help="Display name of the semantic model (e.g. 'Enterprise_Sales_Model')",
    )
    parser.add_argument(
        "--out",
        default=str(DEFAULT_OUT),
        help=f"Output path for model.bim (default: {DEFAULT_OUT})",
    )
    args = parser.parse_args()
    asyncio.run(export_model(args.workspace, args.model, Path(args.out)))


if __name__ == "__main__":
    main()
