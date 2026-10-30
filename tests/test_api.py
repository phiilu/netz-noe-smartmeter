"""Tests for the Netz NÖ API client."""

from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from custom_components.netz_noe_smartmeter.api import (
    NetzNoeApi,
    NetzNoeApiError,
    NetzNoeAuthError,
    NetzNoeLoginError,
    NetzNoeMaintenanceError,
    NetzNoeRateLimitError,
)


def _mock_response(status: int = 200, json_data: object | None = None, text: str = "") -> AsyncMock:
    """Create a mock aiohttp response that arrives on the next event loop iteration."""
    resp = AsyncMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_data)
    resp.text = AsyncMock(return_value=text)

    async def _arrive() -> AsyncMock:
        await asyncio.sleep(0)  # parallel requests all go out before the first answer comes back
        return resp

    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(side_effect=_arrive)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


@pytest.fixture
def mock_session() -> MagicMock:
    """Return a mocked aiohttp.ClientSession that accepts the login."""
    session = MagicMock(spec=aiohttp.ClientSession)
    session.post = MagicMock(return_value=_mock_response(200))
    return session


@pytest.fixture
def api(mock_session: MagicMock) -> NetzNoeApi:
    """Return an API client on the mocked session."""
    return NetzNoeApi(mock_session, "user", "pass")


class TestAuthenticate:
    """Tests for authenticate method."""

    async def test_successful_login(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test successful authentication sends the credentials."""
        await api.authenticate()

        assert mock_session.post.call_args.kwargs["json"] == {"user": "user", "pwd": "pass"}

    async def test_invalid_credentials(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test 401 raises NetzNoeAuthError."""
        mock_session.post = MagicMock(return_value=_mock_response(401))

        with pytest.raises(NetzNoeAuthError, match="Invalid username or password"):
            await api.authenticate()

    async def test_server_error(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test non-200/401 raises NetzNoeLoginError."""
        mock_session.post = MagicMock(return_value=_mock_response(500, text="Internal Server Error"))

        with pytest.raises(NetzNoeLoginError, match="Login failed with status 500"):
            await api.authenticate()

    async def test_maintenance(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test status 999 raises NetzNoeMaintenanceError."""
        mock_session.post = MagicMock(return_value=_mock_response(999))

        with pytest.raises(NetzNoeMaintenanceError, match="under maintenance"):
            await api.authenticate()

    async def test_connection_error(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test connection error raises NetzNoeLoginError."""
        mock_session.post = MagicMock(side_effect=aiohttp.ClientError("timeout"))

        with pytest.raises(NetzNoeLoginError, match="Connection error during login"):
            await api.authenticate()


class TestLogout:
    """Tests for logout method."""

    async def test_logout(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test logout calls the logout endpoint."""
        mock_session.get = MagicMock(return_value=_mock_response(200))

        await api.logout()

        mock_session.get.assert_called_once()

    async def test_logout_handles_error(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test logout doesn't raise on connection error."""
        mock_session.get = MagicMock(side_effect=aiohttp.ClientError())

        await api.logout()  # Should not raise


class TestSession:
    """Tests for the session handling around requests."""

    async def test_logs_in_once_before_first_request(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test the first request logs in and later requests reuse the session."""
        mock_session.get = MagicMock(return_value=_mock_response(200, {"meteredValues": []}))

        await api.get_day("m", date(2025, 1, 15))
        await api.get_day("m", date(2025, 1, 16))

        assert mock_session.post.call_count == 1

    async def test_expired_session_logs_in_again_and_retries(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test a 401 logs in again and repeats the request."""
        day_data = {"meteredValues": [0.1]}
        mock_session.get = MagicMock(side_effect=[_mock_response(401), _mock_response(200, day_data)])

        result = await api.get_day("m", date(2025, 1, 15))

        assert result == day_data
        assert mock_session.post.call_count == 2
        assert mock_session.get.call_count == 2

    async def test_rejected_credentials_on_new_login_raise_auth_error(
        self, mock_session: MagicMock, api: NetzNoeApi
    ) -> None:
        """Test changed credentials surface as NetzNoeAuthError when the session expires."""
        mock_session.post = MagicMock(side_effect=[_mock_response(200), _mock_response(401)])
        mock_session.get = MagicMock(return_value=_mock_response(401))

        with pytest.raises(NetzNoeAuthError):
            await api.get_day("m", date(2025, 1, 15))

    async def test_401_right_after_login_is_not_an_auth_error(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test a 401 after a fresh login raises NetzNoeApiError, so it does not start reauth."""
        mock_session.get = MagicMock(return_value=_mock_response(401))

        with pytest.raises(NetzNoeApiError, match="Session rejected right after login") as exc_info:
            await api.get_day("m", date(2025, 1, 15))

        assert not isinstance(exc_info.value, NetzNoeAuthError)
        assert mock_session.post.call_count == 2

    async def test_parallel_requests_share_logins(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test parallel requests share the first login and the new login after the session expired."""
        responses = [_mock_response(status, {"meteredValues": []}) for status in (401, 401, 401, 200, 200, 200)]
        mock_session.get = MagicMock(side_effect=responses)

        _ = await asyncio.gather(*(api.get_day("m", date(2025, 1, 15)) for _ in range(3)))

        assert mock_session.post.call_count == 2
        assert mock_session.get.call_count == 6

    async def test_parallel_requests_share_a_rejected_login(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test requests waiting for a rejected login get its error instead of sending the credentials again."""
        mock_session.post = MagicMock(side_effect=[_mock_response(200), _mock_response(401)])
        mock_session.get = MagicMock(return_value=_mock_response(401))

        results = await asyncio.gather(
            *(api.get_day("m", date(2025, 1, 15)) for _ in range(3)),
            return_exceptions=True,
        )

        assert all(isinstance(result, NetzNoeAuthError) for result in results)
        assert mock_session.post.call_count == 2
        assert mock_session.get.call_count == 3

    async def test_failed_login_is_repeated_before_the_next_request(
        self, mock_session: MagicMock, api: NetzNoeApi
    ) -> None:
        """Test a failed login stops the request and the next request logs in first."""
        mock_session.post = MagicMock(side_effect=[aiohttp.ClientError("timeout"), _mock_response(200)])
        mock_session.get = MagicMock(return_value=_mock_response(200, {"meteredValues": []}))

        with pytest.raises(NetzNoeLoginError):
            await api.get_day("m", date(2025, 1, 15))
        mock_session.get.assert_not_called()

        await api.get_day("m", date(2025, 1, 15))

        assert mock_session.post.call_count == 2
        assert mock_session.get.call_count == 1


class TestGetDay:
    """Tests for get_day method."""

    async def test_unwraps_list_response(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test that list responses are unwrapped."""
        day_data = {"meteredValues": [0.1, 0.2]}
        mock_session.get = MagicMock(return_value=_mock_response(200, [day_data]))

        result = await api.get_day("meter1", date(2025, 1, 15))

        assert result == day_data

    async def test_dict_response_passthrough(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test that dict responses pass through directly."""
        day_data = {"meteredValues": [0.1]}
        mock_session.get = MagicMock(return_value=_mock_response(200, day_data))

        result = await api.get_day("meter1", date(2025, 1, 15))

        assert result == day_data

    async def test_unexpected_format_raises(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test that unexpected response format raises."""
        mock_session.get = MagicMock(return_value=_mock_response(200, "not a dict"))

        with pytest.raises(NetzNoeApiError, match="Unexpected response format"):
            await api.get_day("meter1", date(2025, 1, 15))


class TestGetRaw:
    """Tests for get_raw method."""

    async def test_unwraps_nested_list(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test nested list [[records]] is unwrapped."""
        records = [{"MeasuredValue": 0.1}]
        mock_session.get = MagicMock(return_value=_mock_response(200, [records]))

        result = await api.get_raw("meter1", date(2025, 1, 1), date(2025, 1, 2))

        assert result == records

    async def test_flat_list_passthrough(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test flat list passes through."""
        records = [{"MeasuredValue": 0.1}]
        mock_session.get = MagicMock(return_value=_mock_response(200, records))

        result = await api.get_raw("meter1", date(2025, 1, 1), date(2025, 1, 2))

        assert result == records

    async def test_non_list_returns_empty(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test non-list response returns empty list."""
        mock_session.get = MagicMock(return_value=_mock_response(200, "not a list"))

        result = await api.get_raw("meter1", date(2025, 1, 1), date(2025, 1, 2))

        assert result == []


class TestGetWeekAndYear:
    """Tests for get_week and get_year methods."""

    async def test_get_week_unwraps(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test get_week unwraps list response."""
        week_data = {"meteredValues": [1.0]}
        mock_session.get = MagicMock(return_value=_mock_response(200, [week_data]))

        result = await api.get_week("m", date(2025, 1, 13), date(2025, 1, 20))

        assert result == week_data

    async def test_get_year_unwraps(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test get_year unwraps list response."""
        year_data = {"values": [10.0] * 12}
        mock_session.get = MagicMock(return_value=_mock_response(200, [year_data]))

        result = await api.get_year("m", 2025)

        assert result == year_data

    async def test_get_year_unexpected_format(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test get_year raises on unexpected format."""
        mock_session.get = MagicMock(return_value=_mock_response(200, 42))

        with pytest.raises(NetzNoeApiError, match="Unexpected response format"):
            await api.get_year("m", 2025)

    async def test_get_week_unexpected_format(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test get_week raises on unexpected format."""
        mock_session.get = MagicMock(return_value=_mock_response(200, "not a dict"))

        with pytest.raises(NetzNoeApiError, match="Unexpected response format"):
            await api.get_week("m", date(2025, 1, 13), date(2025, 1, 20))


class TestGetErrorPaths:
    """Tests for _get method error paths."""

    async def test_non_200_includes_body(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test non-200 non-401 response includes body in error message."""
        mock_session.get = MagicMock(return_value=_mock_response(500, text="Internal Server Error"))

        with pytest.raises(NetzNoeApiError, match="API error 500"):
            await api.get_day("m", date(2025, 1, 15))

    async def test_connection_error_wraps(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test connection error wraps in NetzNoeApiError."""
        mock_session.get = MagicMock(side_effect=aiohttp.ClientError("connect timeout"))

        with pytest.raises(NetzNoeApiError, match="Connection error"):
            await api.get_day("m", date(2025, 1, 15))

    async def test_429_raises_rate_limit_error(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test 429 response raises NetzNoeRateLimitError."""
        mock_session.get = MagicMock(return_value=_mock_response(429))

        with pytest.raises(NetzNoeRateLimitError, match="rate limit"):
            await api.get_day("m", date(2025, 1, 15))

    async def test_999_raises_maintenance_error(self, mock_session: MagicMock, api: NetzNoeApi) -> None:
        """Test 999 response raises NetzNoeMaintenanceError."""
        mock_session.get = MagicMock(return_value=_mock_response(999))

        with pytest.raises(NetzNoeMaintenanceError, match="under maintenance"):
            await api.get_day("m", date(2025, 1, 15))

    async def test_rate_limit_error_is_api_error_subclass(self) -> None:
        """Test NetzNoeRateLimitError is a subclass of NetzNoeApiError."""
        assert issubclass(NetzNoeRateLimitError, NetzNoeApiError)
