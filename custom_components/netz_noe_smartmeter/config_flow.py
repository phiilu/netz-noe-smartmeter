"""Config flow for Netz NÖ Smart Meter integration."""

from __future__ import annotations

import logging
from typing import Any, cast, override

import aiohttp
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant

from .api import NetzNoeApi, NetzNoeApiError, NetzNoeAuthError
from .const import CONF_METER_ID, DOMAIN

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_USERNAME): str,
        vol.Required(CONF_PASSWORD): str,
        vol.Required(CONF_METER_ID): str,
    }
)


async def validate_input(_hass: HomeAssistant, data: dict[str, str]) -> dict[str, str]:
    """Validate the user input by attempting to log in."""
    jar = aiohttp.CookieJar(unsafe=True)
    async with aiohttp.ClientSession(cookie_jar=jar) as session:
        api = NetzNoeApi(session)
        _ = await api.authenticate(data[CONF_USERNAME], data[CONF_PASSWORD])
        _ = await api.logout()

    # Return a title for the config entry
    meter_id = data[CONF_METER_ID]
    short_id = meter_id[-10:] if len(meter_id) > 10 else meter_id
    return {"title": f"Smart Meter {short_id}"}


class NetzNoeConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Netz NÖ Smart Meter."""

    VERSION: int = 1

    @override
    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,  # pyright: ignore[reportExplicitAny]
    ) -> config_entries.ConfigFlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            username_raw = cast(object, user_input.get(CONF_USERNAME))
            password_raw = cast(object, user_input.get(CONF_PASSWORD))
            meter_id_raw = cast(object, user_input.get(CONF_METER_ID))

            if not (isinstance(username_raw, str) and isinstance(password_raw, str) and isinstance(meter_id_raw, str)):
                errors["base"] = "unknown"
            else:
                validated_input: dict[str, str] = {
                    CONF_USERNAME: username_raw,
                    CONF_PASSWORD: password_raw,
                    CONF_METER_ID: meter_id_raw,
                }

                # Prevent duplicate entries for the same meter
                _ = await self.async_set_unique_id(validated_input[CONF_METER_ID])
                _ = self._abort_if_unique_id_configured()

                try:
                    info = await validate_input(self.hass, validated_input)
                except NetzNoeAuthError:
                    errors["base"] = "invalid_auth"
                except NetzNoeApiError:
                    errors["base"] = "cannot_connect"
                except Exception:
                    _LOGGER.exception("Unexpected exception")
                    errors["base"] = "unknown"
                else:
                    return self.async_create_entry(title=info["title"], data=validated_input)

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )

    async def async_step_reauth(
        self,
        _entry_data: dict[str, Any],  # pyright: ignore[reportExplicitAny]
    ) -> config_entries.ConfigFlowResult:
        """Handle reauth when credentials expire."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, str] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Handle reauth credential confirmation."""
        errors: dict[str, str] = {}
        reauth_entry = self._get_reauth_entry()

        _ = await self.async_set_unique_id(reauth_entry.unique_id)
        _ = self._abort_if_unique_id_mismatch()

        if user_input is not None:
            try:
                _ = await validate_input(
                    self.hass,
                    {
                        CONF_USERNAME: user_input[CONF_USERNAME],
                        CONF_PASSWORD: user_input[CONF_PASSWORD],
                        CONF_METER_ID: reauth_entry.data[CONF_METER_ID],
                    },
                )
            except NetzNoeAuthError:
                errors["base"] = "invalid_auth"
            except NetzNoeApiError:
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected exception during reauth")
                errors["base"] = "unknown"
            else:
                return self.async_update_reload_and_abort(
                    reauth_entry,
                    data={
                        **reauth_entry.data,
                        CONF_USERNAME: user_input[CONF_USERNAME],
                        CONF_PASSWORD: user_input[CONF_PASSWORD],
                    },
                )

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_USERNAME): str,
                    vol.Required(CONF_PASSWORD): str,
                }
            ),
            errors=errors,
        )
