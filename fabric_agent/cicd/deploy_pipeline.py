from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from loguru import logger

from fabric_agent.api.fabric_client import FabricApiClient, FabricApiError


@dataclass
class DeployResult:
    deployment_pipeline_id: str
    deployment_id: str
    status: str
    operation: dict[str, Any]


def _normalize_status(s: str | None) -> str:
    return (s or "").strip().lower()


async def deploy_stage_content_and_wait(
    client: FabricApiClient,
    *,
    deployment_pipeline_id: str,
    source_stage_id: str,
    target_stage_id: str,
    items: list[dict[str, str]] | None = None,
    note: str | None = None,
    allow_cross_region_deployment: bool = False,
    timeout_s: int = 1800,
    poll_s: float = 5.0,
) -> DeployResult:
    """Deploy between Fabric deployment pipeline stages and wait for completion.

    Uses POST /v1/deploymentPipelines/{id}/deploy and then polls:
      GET /v1/deploymentPipelines/{id}/operations/{deploymentId}

    This is the "Option 2" CI step after PR approval.
    """
    body: dict[str, Any] = {
        "sourceStageId": source_stage_id,
        "targetStageId": target_stage_id,
    }
    if note:
        body["note"] = note
    if items:
        # Deploy Stage Content expects: items: [{sourceItemId, itemType}, ...]
        body["items"] = items
    if allow_cross_region_deployment:
        body["options"] = {"allowCrossRegionDeployment": True}

    resp = await client.deploy_stage_content_raw(deployment_pipeline_id, body)
    if resp.status_code == 200:
        data = resp.json()
        dep_id = str(data.get("id") or data.get("deploymentId") or "")
        return DeployResult(deployment_pipeline_id, dep_id, str(data.get("status", "Succeeded")), data)

    if resp.status_code != 202:
        raise FabricApiError("Deploy failed", resp.status_code, resp.text)

    deployment_id = resp.headers.get("deployment-id") or resp.headers.get("Deployment-Id") or ""
    if not deployment_id:
        # Some tenants might not return deployment-id; fall back to x-ms-operation-id
        deployment_id = resp.headers.get("x-ms-operation-id") or ""

    if not deployment_id:
        raise FabricApiError("Deploy accepted but missing deployment-id header", resp.status_code, resp.text)

    start = time.time()
    last_status = "running"
    while True:
        op = await client.get_deployment_pipeline_operation(deployment_pipeline_id, deployment_id)
        status = _normalize_status(str(op.get("status")))
        last_status = status or last_status
        if status in ("succeeded", "failed", "cancelled"):
            return DeployResult(deployment_pipeline_id, deployment_id, status, op)
        if time.time() - start > timeout_s:
            raise FabricApiError(f"Deployment timed out after {timeout_s}s (last_status={last_status})")
        await asyncio.sleep(poll_s)


async def list_stage_items_all(
    client: FabricApiClient, *, deployment_pipeline_id: str, stage_id: str
) -> list[dict[str, Any]]:
    """Fetch all stage items across continuation tokens."""
    all_items: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        data = await client.list_deployment_pipeline_stage_items(
            deployment_pipeline_id, stage_id, continuation_token=token
        )
        batch = data.get("value") or []
        all_items.extend(batch)
        token = data.get("continuationToken")
        if not token:
            break
    return all_items
