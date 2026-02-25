import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from fabric_agent.api.fabric_client import FabricApiError
from fabric_agent.healing.models import (
    Anomaly,
    AnomalyType,
    HealAction,
    HealingPlan,
    RiskLevel,
)

# Ensure the project root (containing the fabric_agent package) is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Ensure the tests directory is importable (for `from conftest import ...` in test files)
TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))


# =============================================================================
# Realistic Fabric API error bodies (from Microsoft docs)
# =============================================================================

FABRIC_ERROR_503 = {
    "errorCode": "ServiceUnavailable",
    "message": "The service is temporarily unavailable. Please try again later.",
    "requestId": "550e8400-e29b-41d4-a716-446655440004",
}

FABRIC_ERROR_429 = {
    "errorCode": "RequestBlocked",
    "message": "Request is blocked by the upstream service until: 2026-02-24 15:30:45 (UTC)",
    "requestId": "550e8400-e29b-41d4-a716-446655440003",
}

FABRIC_ERROR_401 = {
    "errorCode": "TokenExpired",
    "message": "The access token has expired.",
    "requestId": "550e8400-e29b-41d4-a716-446655440000",
}

FABRIC_ERROR_403 = {
    "errorCode": "InsufficientPrivileges",
    "message": "The caller does not have the required permissions to access the resource.",
    "requestId": "550e8400-e29b-41d4-a716-446655440002",
}

FABRIC_ERROR_404 = {
    "errorCode": "EntityNotFound",
    "message": "The requested resource could not be found.",
    "requestId": "550e8400-e29b-41d4-a716-446655440005",
}

FABRIC_ERROR_409 = {
    "errorCode": "Conflict",
    "message": "The resource already exists.",
    "requestId": "550e8400-e29b-41d4-a716-446655440006",
}


# =============================================================================
# Helper factories
# =============================================================================

def make_fabric_error(status_code: int, body: dict | None = None) -> FabricApiError:
    """Create a FabricApiError matching real Fabric REST API responses."""
    default_bodies = {
        401: FABRIC_ERROR_401,
        403: FABRIC_ERROR_403,
        404: FABRIC_ERROR_404,
        409: FABRIC_ERROR_409,
        429: FABRIC_ERROR_429,
        503: FABRIC_ERROR_503,
    }
    body = body or default_bodies.get(status_code, {"errorCode": "Unknown"})
    return FabricApiError(
        message=body.get("message", f"HTTP {status_code}"),
        status_code=status_code,
        response_body=body,
    )


def make_anomaly(
    anomaly_type: AnomalyType = AnomalyType.BROKEN_SHORTCUT,
    asset_name: str = "test_shortcut",
    workspace_id: str = "ws-001",
    can_auto_heal: bool = True,
    metadata: dict | None = None,
) -> Anomaly:
    """Create an Anomaly with sensible defaults for testing."""
    return Anomaly(
        anomaly_type=anomaly_type,
        severity=RiskLevel.HIGH,
        asset_id=f"asset-{asset_name}",
        asset_name=asset_name,
        workspace_id=workspace_id,
        details=f"Test anomaly: {anomaly_type.value}",
        can_auto_heal=can_auto_heal,
        metadata=metadata or {},
    )


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def mock_fabric_client():
    """AsyncMock of FabricApiClient for unit tests — no real API calls."""
    from fabric_agent.api.fabric_client import FabricApiClient

    client = AsyncMock(spec=FabricApiClient)
    # Default happy-path returns
    client.get.return_value = {"value": []}
    client.post.return_value = {}
    client.post_with_lro.return_value = {}
    # SafeRenameEngine uses these higher-level methods (not on spec)
    client.update_report_definition = AsyncMock(return_value={})
    client.update_semantic_model_definition = AsyncMock(return_value={})
    return client


@pytest.fixture
def mock_memory_manager():
    """AsyncMock of MemoryManager for unit tests."""
    from fabric_agent.storage.memory_manager import MemoryManager

    mm = AsyncMock(spec=MemoryManager)
    mm.initialize.return_value = None
    mm.record_state.return_value = None
    return mm
