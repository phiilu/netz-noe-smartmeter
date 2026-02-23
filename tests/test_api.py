"""Tests for the Netz NÖ API client."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from custom_components.netz_noe_smartmeter.api import (
    NetzNoeApi,
    NetzNoeApiError,
    NetzNoeAuthError,
    NetzNoeRateLimitError,
)


@pytest.fixture
def mock_session() -> MagicMock:
    """Return a mocked aiohttp.ClientSession."""
    return MagicMock(spec=aiohttp.ClientSession)


def _mock_response(status: int = 200, json_data: object | None = None, text: str = "") -> AsyncMock:
    """Create a mock aiohttp response."""
    resp = AsyncMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_data)
    resp.text = AsyncMock(return_value=text)
    # Support async context manager
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=resp)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


class TestAuthenticate:
    """Tests for authenticate method."""

    async def test_successful_login(self, mock_session: MagicMock) -> None:
        """Test successful authentication."""
        mock_session.post = MagicMock(return_value=_mock_response(200))
        api = NetzNoeApi(mock_session)

        result = await api.authenticate("user", "pass")

        assert result is True
        assert api.is_authenticated is True

    async def test_invalid_credentials(self, mock_session: MagicMock) -> None:
        """Test 401 raises NetzNoeAuthError."""
        mock_session.post = MagicMock(return_value=_mock_response(401))
        api = NetzNoeApi(mock_session)

        with pytest.raises(NetzNoeAuthError, match="Invalid username or password"):
            await api.authenticate("user", "wrong")

        assert api.is_authenticated is False

    async def test_server_error(self, mock_session: MagicMock) -> None:
        """Test non-200/401 raises NetzNoeApiError."""
        mock_session.post = MagicMock(return_value=_mock_response(500, text="Internal Server Error"))
        api = NetzNoeApi(mock_session)

        with pytest.raises(NetzNoeApiError, match="Login failed with status 500"):
            await api.authenticate("user", "pass")

    async def test_connection_error(self, mock_session: MagicMock) -> None:
        """Test connection error raises NetzNoeApiError."""
        mock_session.post = MagicMock(side_effect=aiohttp.ClientError("timeout"))
        api = NetzNoeApi(mock_session)

        with pytest.raises(NetzNoeApiError, match="Connection error during login"):
            await api.authenticate("user", "pass")


class TestLogout:
    """Tests for logout method."""

    async def test_logout_clears_auth(self, mock_session: MagicMock) -> None:
        """Test logout marks session as unauthenticated."""
        mock_session.get = MagicMock(return_value=_mock_response(200))
        api = NetzNoeApi(mock_session)
        api._authenticated = True

        await api.logout()

        assert api.is_authenticated is False

    async def test_logout_handles_error(self, mock_session: MagicMock) -> None:
        """Test logout doesn't raise on connection error."""
        mock_session.get = MagicMock(side_effect=aiohttp.ClientError())
        api = NetzNoeApi(mock_session)
        api._authenticated = True

        await api.logout()  # Should not raise

        assert api.is_authenticated is False


class TestInvalidateSession:
    """Tests for invalidate_session method."""

    def test_invalidate_session(self, mock_session: MagicMock) -> None:
        """Test invalidate_session clears auth state."""
        api = NetzNoeApi(mock_session)
        api._authenticated = True

        api.invalidate_session()

        assert api.is_authenticated is False


class TestGetDay:
    """Tests for get_day method."""

    async def test_unwraps_list_response(self, mock_session: MagicMock) -> None:
        """Test that list responses are unwrapped."""
        day_data = {"meteredValues": [0.1, 0.2]}
        mock_session.get = MagicMock(return_value=_mock_response(200, [day_data]))
        api = NetzNoeApi(mock_session)

        result = await api.get_day("meter1", date(2025, 1, 15))

        assert result == day_data

    async def test_dict_response_passthrough(self, mock_session: MagicMock) -> None:
        """Test that dict responses pass through directly."""
        day_data = {"meteredValues": [0.1]}
        mock_session.get = MagicMock(return_value=_mock_response(200, day_data))
        api = NetzNoeApi(mock_session)

        result = await api.get_day("meter1", date(2025, 1, 15))

        assert result == day_data

    async def test_unexpected_format_raises(self, mock_session: MagicMock) -> None:
        """Test that unexpected response format raises."""
        mock_session.get = MagicMock(return_value=_mock_response(200, "not a dict"))
        api = NetzNoeApi(mock_session)

        with pytest.raises(NetzNoeApiError, match="Unexpected response format"):
            await api.get_day("meter1", date(2025, 1, 15))

    async def test_401_raises_auth_error(self, mock_session: MagicMock) -> None:
        """Test 401 response raises NetzNoeAuthError."""
        mock_session.get = MagicMock(return_value=_mock_response(401))
        api = NetzNoeApi(mock_session)

        with pytest.raises(NetzNoeAuthError, match="Session expired"):
            await api.get_day("meter1", date(2025, 1, 15))


class TestGetRaw:
    """Tests for get_raw method."""

    async def test_unwraps_nested_list(self, mock_session: MagicMock) -> None:
        """Test nested list [[records]] is unwrapped."""
        records = [{"MeasuredValue": 0.1}]
        mock_session.get = MagicMock(return_value=_mock_response(200, [records]))
        api = NetzNoeApi(mock_session)

        result = await api.get_raw("meter1", date(2025, 1, 1), date(2025, 1, 2))

        assert result == records

    async def test_flat_list_passthrough(self, mock_session: MagicMock) -> None:
        """Test flat list passes through."""
        records = [{"MeasuredValue": 0.1}]
        mock_session.get = MagicMock(return_value=_mock_response(200, records))
        api = NetzNoeApi(mock_session)

        result = await api.get_raw("meter1", date(2025, 1, 1), date(2025, 1, 2))

        assert result == records

    async def test_non_list_returns_empty(self, mock_session: MagicMock) -> None:
        """Test non-list response returns empty list."""
        mock_session.get = MagicMock(return_value=_mock_response(200, "not a list"))
        api = NetzNoeApi(mock_session)

        result = await api.get_raw("meter1", date(2025, 1, 1), date(2025, 1, 2))

        assert result == []


class TestGetWeekAndYear:
    """Tests for get_week and get_year methods."""

    async def test_get_week_unwraps(self, mock_session: MagicMock) -> None:
        """Test get_week unwraps list response."""
        week_data = {"meteredValues": [1.0]}
        mock_session.get = MagicMock(return_value=_mock_response(200, [week_data]))
        api = NetzNoeApi(mock_session)

        result = await api.get_week("m", date(2025, 1, 13), date(2025, 1, 19))

        assert result == week_data

    async def test_get_year_unwraps(self, mock_session: MagicMock) -> None:
        """Test get_year unwraps list response."""
        year_data = {"values": [10.0] * 12}
        mock_session.get = MagicMock(return_value=_mock_response(200, [year_data]))
        api = NetzNoeApi(mock_session)

        result = await api.get_year("m", 2025)

        assert result == year_data

    async def test_get_year_unexpected_format(self, mock_session: MagicMock) -> None:
        """Test get_year raises on unexpected format."""
        mock_session.get = MagicMock(return_value=_mock_response(200, 42))
        api = NetzNoeApi(mock_session)

        with pytest.raises(NetzNoeApiError, match="Unexpected response format"):
            await api.get_year("m", 2025)

    async def test_get_week_unexpected_format(self, mock_session: MagicMock) -> None:
        """Test get_week raises on unexpected format."""
        mock_session.get = MagicMock(return_value=_mock_response(200, "not a dict"))
        api = NetzNoeApi(mock_session)

        with pytest.raises(NetzNoeApiError, match="Unexpected response format"):
            await api.get_week("m", date(2025, 1, 13), date(2025, 1, 19))


class TestExtendSession:
    """Tests for extend_session method."""

    async def test_extend_session_success(self, mock_session: MagicMock) -> None:
        """Test successful session extension."""
        mock_session.get = MagicMock(return_value=_mock_response(200))
        api = NetzNoeApi(mock_session)
        api._authenticated = True

        await api.extend_session()

        assert api.is_authenticated is True

    async def test_extend_session_non_200(self, mock_session: MagicMock) -> None:
        """Test non-200 response marks session as unauthenticated."""
        mock_session.get = MagicMock(return_value=_mock_response(403))
        api = NetzNoeApi(mock_session)
        api._authenticated = True

        await api.extend_session()

        assert api.is_authenticated is False

    async def test_extend_session_connection_error(self, mock_session: MagicMock) -> None:
        """Test connection error marks session as unauthenticated."""
        mock_session.get = MagicMock(side_effect=aiohttp.ClientError("timeout"))
        api = NetzNoeApi(mock_session)
        api._authenticated = True

        await api.extend_session()

        assert api.is_authenticated is False


class TestGetErrorPaths:
    """Tests for _get method error paths."""

    async def test_non_200_includes_body(self, mock_session: MagicMock) -> None:
        """Test non-200 non-401 response includes body in error message."""
        mock_session.get = MagicMock(return_value=_mock_response(500, text="Internal Server Error"))
        api = NetzNoeApi(mock_session)

        with pytest.raises(NetzNoeApiError, match="API error 500"):
            await api.get_day("m", date(2025, 1, 15))

    async def test_connection_error_wraps(self, mock_session: MagicMock) -> None:
        """Test connection error wraps in NetzNoeApiError."""
        mock_session.get = MagicMock(side_effect=aiohttp.ClientError("connect timeout"))
        api = NetzNoeApi(mock_session)

        with pytest.raises(NetzNoeApiError, match="Connection error"):
            await api.get_day("m", date(2025, 1, 15))

    async def test_429_raises_rate_limit_error(self, mock_session: MagicMock) -> None:
        """Test 429 response raises NetzNoeRateLimitError."""
        mock_session.get = MagicMock(return_value=_mock_response(429))
        api = NetzNoeApi(mock_session)

        with pytest.raises(NetzNoeRateLimitError, match="rate limit"):
            await api.get_day("m", date(2025, 1, 15))

    async def test_rate_limit_error_is_api_error_subclass(self) -> None:
        """Test NetzNoeRateLimitError is a subclass of NetzNoeApiError."""
        assert issubclass(NetzNoeRateLimitError, NetzNoeApiError)
