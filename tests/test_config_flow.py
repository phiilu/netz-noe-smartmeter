"""Tests for config flow."""

from __future__ import annotations

from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.netz_noe_smartmeter.api import NetzNoeApiError, NetzNoeAuthError
from custom_components.netz_noe_smartmeter.config_flow import NetzNoeConfigFlow, validate_input
from custom_components.netz_noe_smartmeter.const import CONF_METER_ID, DOMAIN

from .conftest import MOCK_CONFIG_DATA, MOCK_METER_ID

PATCH_VALIDATE = "custom_components.netz_noe_smartmeter.config_flow.validate_input"


async def test_user_flow_success(hass: HomeAssistant) -> None:
    """Test successful user config flow."""
    with patch(PATCH_VALIDATE, return_value={"title": "Smart Meter 0000654321"}):
        result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
        assert result["type"] is FlowResultType.FORM  # type: ignore[typeddict-item]

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            MOCK_CONFIG_DATA,  # type: ignore[typeddict-item]
        )

    assert result["type"] is FlowResultType.CREATE_ENTRY  # type: ignore[typeddict-item]
    assert result["title"] == "Smart Meter 0000654321"  # type: ignore[typeddict-item]
    assert result["data"] == MOCK_CONFIG_DATA  # type: ignore[typeddict-item]


async def test_user_flow_invalid_auth(hass: HomeAssistant) -> None:
    """Test config flow with invalid credentials."""
    with patch(PATCH_VALIDATE, side_effect=NetzNoeAuthError("bad password")):
        result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            MOCK_CONFIG_DATA,  # type: ignore[typeddict-item]
        )

    assert result["type"] is FlowResultType.FORM  # type: ignore[typeddict-item]
    assert result["errors"] == {"base": "invalid_auth"}  # type: ignore[typeddict-item]


async def test_user_flow_cannot_connect(hass: HomeAssistant) -> None:
    """Test config flow with connection error."""
    with patch(PATCH_VALIDATE, side_effect=NetzNoeApiError("timeout")):
        result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            MOCK_CONFIG_DATA,  # type: ignore[typeddict-item]
        )

    assert result["type"] is FlowResultType.FORM  # type: ignore[typeddict-item]
    assert result["errors"] == {"base": "cannot_connect"}  # type: ignore[typeddict-item]


async def test_user_flow_unknown_error(hass: HomeAssistant) -> None:
    """Test config flow with unexpected error."""
    with patch(PATCH_VALIDATE, side_effect=RuntimeError("unexpected")):
        result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            MOCK_CONFIG_DATA,  # type: ignore[typeddict-item]
        )

    assert result["type"] is FlowResultType.FORM  # type: ignore[typeddict-item]
    assert result["errors"] == {"base": "unknown"}  # type: ignore[typeddict-item]


async def test_validate_input_success(hass: HomeAssistant) -> None:
    """Test validate_input performs login/logout and returns title."""
    session = MagicMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session

    with (
        patch(
            "custom_components.netz_noe_smartmeter.config_flow.aiohttp.ClientSession",
            return_value=session_cm,
        ) as mock_client_session,
        patch("custom_components.netz_noe_smartmeter.config_flow.NetzNoeApi") as mock_api_cls,
    ):
        api = mock_api_cls.return_value
        api.authenticate = AsyncMock(return_value=True)
        api.logout = AsyncMock(return_value=None)

        result = await validate_input(
            hass,
            {
                "username": "user@example.com",
                "password": "secret",
                CONF_METER_ID: MOCK_METER_ID,
            },
        )

    assert result == {"title": "Smart Meter 0000654321"}
    mock_client_session.assert_called_once()
    api.authenticate.assert_awaited_once_with("user@example.com", "secret")
    api.logout.assert_awaited_once()


async def test_user_flow_invalid_input_types(hass: HomeAssistant) -> None:
    """Test config flow returns unknown for non-string inputs."""
    init_result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
    flow_id = init_result["flow_id"]  # type: ignore[typeddict-item]
    flow = cast(NetzNoeConfigFlow, hass.config_entries.flow._progress[flow_id])  # type: ignore[attr-defined]

    result = await flow.async_step_user(
        {
            "username": 123,
            "password": 456,
            CONF_METER_ID: 789,
        }
    )

    assert result["type"] is FlowResultType.FORM  # type: ignore[typeddict-item]
    assert result["errors"] == {"base": "unknown"}  # type: ignore[typeddict-item]


async def test_user_flow_duplicate_meter(hass: HomeAssistant) -> None:
    """Test config flow aborts on duplicate meter ID."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Existing",
        data=MOCK_CONFIG_DATA,
        unique_id=MOCK_METER_ID,
    )
    entry.add_to_hass(hass)

    with patch(PATCH_VALIDATE, return_value={"title": "Smart Meter"}):
        result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            MOCK_CONFIG_DATA,  # type: ignore[typeddict-item]
        )

    assert result["type"] is FlowResultType.ABORT  # type: ignore[typeddict-item]
    assert result["reason"] == "already_configured"  # type: ignore[typeddict-item]


async def test_reauth_flow_success(hass: HomeAssistant) -> None:
    """Test successful reauth flow updates credentials."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Smart Meter",
        data=MOCK_CONFIG_DATA,
        unique_id=MOCK_METER_ID,
    )
    entry.add_to_hass(hass)

    with patch(PATCH_VALIDATE, return_value={"title": "Smart Meter"}):
        result = await entry.start_reauth_flow(hass)
        assert result["type"] is FlowResultType.FORM  # type: ignore[typeddict-item]
        assert result["step_id"] == "reauth_confirm"  # type: ignore[typeddict-item]

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],  # type: ignore[typeddict-item]
            {"username": "new@example.com", "password": "new-pass"},
        )

    assert result["type"] is FlowResultType.ABORT  # type: ignore[typeddict-item]
    assert result["reason"] == "reauth_successful"  # type: ignore[typeddict-item]
    assert entry.data["username"] == "new@example.com"
    assert entry.data["password"] == "new-pass"
    # meter_id preserved
    assert entry.data[CONF_METER_ID] == MOCK_METER_ID


async def test_reauth_flow_invalid_auth(hass: HomeAssistant) -> None:
    """Test reauth flow shows error on invalid credentials."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Smart Meter",
        data=MOCK_CONFIG_DATA,
        unique_id=MOCK_METER_ID,
    )
    entry.add_to_hass(hass)

    with patch(PATCH_VALIDATE, side_effect=NetzNoeAuthError("bad")):
        result = await entry.start_reauth_flow(hass)
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],  # type: ignore[typeddict-item]
            {"username": "user", "password": "wrong"},
        )

    assert result["type"] is FlowResultType.FORM  # type: ignore[typeddict-item]
    assert result["errors"] == {"base": "invalid_auth"}  # type: ignore[typeddict-item]
    await hass.async_block_till_done()


async def test_reauth_flow_cannot_connect(hass: HomeAssistant) -> None:
    """Test reauth flow shows error on connection failure."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Smart Meter",
        data=MOCK_CONFIG_DATA,
        unique_id=MOCK_METER_ID,
    )
    entry.add_to_hass(hass)

    with patch(PATCH_VALIDATE, side_effect=NetzNoeApiError("timeout")):
        result = await entry.start_reauth_flow(hass)
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],  # type: ignore[typeddict-item]
            {"username": "user", "password": "pass"},
        )

    assert result["type"] is FlowResultType.FORM  # type: ignore[typeddict-item]
    assert result["errors"] == {"base": "cannot_connect"}  # type: ignore[typeddict-item]


async def test_reauth_flow_unknown_error(hass: HomeAssistant) -> None:
    """Test reauth flow shows error on unexpected exception."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Smart Meter",
        data=MOCK_CONFIG_DATA,
        unique_id=MOCK_METER_ID,
    )
    entry.add_to_hass(hass)

    with patch(PATCH_VALIDATE, side_effect=RuntimeError("boom")):
        result = await entry.start_reauth_flow(hass)
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],  # type: ignore[typeddict-item]
            {"username": "user", "password": "pass"},
        )

    assert result["type"] is FlowResultType.FORM  # type: ignore[typeddict-item]
    assert result["errors"] == {"base": "unknown"}  # type: ignore[typeddict-item]
