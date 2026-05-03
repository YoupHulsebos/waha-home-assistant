"""Config flow for WAHA WhatsApp integration."""
import logging
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.const import CONF_NAME
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import config_entry_oauth2_flow
import traceback
import aiohttp
import json

from .const import (
    DOMAIN,
    CONF_BASE_URL,
    CONF_API_KEY,
    CONF_PHONE_NUMBERS,
    CONF_SESSION_NAME,
    DEFAULT_SESSION_NAME,
)

_LOGGER = logging.getLogger(__name__)

def validate_url(url: str) -> str:
    """Validate URL format."""
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise vol.Invalid("Invalid URL format")
    return url.rstrip("/")

def validate_phone_number(phone: str) -> str:
    """Validate phone number format."""
    # Remove spaces, dashes, and other non-digit characters except +
    clean_phone = phone.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    
    # Check if it starts with + and contains only digits after that
    if clean_phone.startswith("+") and clean_phone[1:].isdigit() and len(clean_phone) >= 10:
        return clean_phone
    # If it doesn't start with +, but is all digits, add +
    elif clean_phone.isdigit() and len(clean_phone) >= 10:
        return f"+{clean_phone}"
    else:
        raise vol.Invalid(f"Invalid phone number format: {phone}")

class WahaConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for WAHA WhatsApp."""

    VERSION = 1
    CONNECTION_CLASS = config_entries.CONN_CLASS_CLOUD_POLL

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._errors: Dict[str, str] = {}

    async def async_step_user(self, user_input: Optional[Dict[str, Any]] = None) -> FlowResult:
        """Handle a flow initiated by the user."""
        self._errors = {}

        if user_input is not None:
            try:
                # Validate URL
                user_input[CONF_BASE_URL] = validate_url(user_input[CONF_BASE_URL])

                # Test connection
                if await self._test_connection(user_input):
                    # Initialize with empty phone numbers list
                    user_input[CONF_PHONE_NUMBERS] = []
                    
                    return self.async_create_entry(
                        title=f"WAHA WhatsApp ({user_input[CONF_SESSION_NAME]})",
                        data=user_input,
                    )
                self._errors["base"] = "cannot_connect"
            except vol.Invalid as err:
                _LOGGER.error("Validation error: %s", err)
                self._errors["base"] = "invalid_format"
            except Exception as err:  # pylint: disable=broad-except
                _LOGGER.error("Unexpected error: %s", err)
                self._errors["base"] = "unknown"

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required(CONF_BASE_URL): str,
                vol.Optional(CONF_API_KEY): str,
                vol.Optional(CONF_SESSION_NAME, default=DEFAULT_SESSION_NAME): str,
            }),
            errors=self._errors,
        )

    async def _test_connection(self, data: Dict[str, Any]) -> bool:
        """Test connection to WAHA instance."""
        headers = {"accept": "application/json"}
        
        if api_key := data.get(CONF_API_KEY):
            headers["X-Api-Key"] = api_key
            
        endpoints = ["api/server/version", "api/version"]

        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession() as session:
                for endpoint in endpoints:
                    url = f"{data[CONF_BASE_URL]}/{endpoint}"
                    async with session.get(url, headers=headers, timeout=timeout) as resp:
                        response_text = await resp.text()

                        if resp.status != 200:
                            _LOGGER.warning(
                                "WAHA server returned status %s for %s: %s",
                                resp.status,
                                endpoint,
                                response_text,
                            )
                            continue

                        response_data: Optional[Dict[str, Any]] = None
                        try:
                            response_data = json.loads(response_text)
                        except json.JSONDecodeError:
                            _LOGGER.warning(
                                "WAHA server returned non-JSON payload for %s (content-type: %s)",
                                endpoint,
                                resp.headers.get("Content-Type", "unknown"),
                            )
                            continue

                        if isinstance(response_data, dict) and "version" in response_data:
                            _LOGGER.debug(
                                "WAHA connection test successful via %s. Version: %s",
                                endpoint,
                                response_data.get("version"),
                            )
                            return True

                        _LOGGER.warning(
                            "WAHA response for %s does not contain version field: %s",
                            endpoint,
                            response_data,
                        )
        except aiohttp.ClientError as err:
            _LOGGER.error("Failed to connect to WAHA server: %s", err)
        except TimeoutError as err:
            _LOGGER.error("Connection to WAHA server timed out: %r", err)
        except Exception as err:  # pylint: disable=broad-except
            _LOGGER.error(
                "Unexpected error testing WAHA connection: %r\n%s",
                err,
                traceback.format_exc(),
            )
        return False

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: config_entries.ConfigEntry) -> "WahaOptionsFlowHandler":
        """Get the options flow for this handler."""
        return WahaOptionsFlowHandler(config_entry)

    @staticmethod
    @callback
    def async_get_suggested_values():
        return {CONF_API_KEY: {"sensitive": True}}

class WahaOptionsFlowHandler(config_entries.OptionsFlow):
    """Handle WAHA options."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        self.config_entry = config_entry
        self._errors: Dict[str, str] = {}

    async def async_step_init(
        self, user_input: Optional[Dict[str, Any]] = None
    ) -> FlowResult:
        """Handle options flow."""
        self._errors = {}

        if user_input is not None:
            if user_input.get("action") == "add_phone":
                return await self.async_step_add_phone()
            elif user_input.get("action") == "remove_phone":
                return await self.async_step_remove_phone()
            elif user_input.get("action") == "done":
                return self.async_create_entry(title="", data={})

        # Get current phone numbers
        current_phones = self.config_entry.data.get(CONF_PHONE_NUMBERS, [])
        
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema({
                vol.Required("action", default="done"): vol.In({
                    "add_phone": "Add Phone Number",
                    "remove_phone": "Remove Phone Number" if current_phones else None,
                    "done": "Done"
                })
            }),
            description_placeholders={
                "current_phones": "\n".join([f"• {phone}" for phone in current_phones]) if current_phones else "No phone numbers configured"
            },
        )

    async def async_step_add_phone(
        self, user_input: Optional[Dict[str, Any]] = None
    ) -> FlowResult:
        """Handle adding a phone number."""
        self._errors = {}

        if user_input is not None:
            try:
                # Validate phone number
                phone_number = validate_phone_number(user_input["phone_number"])
                
                # Get current phone numbers
                current_phones = list(self.config_entry.data.get(CONF_PHONE_NUMBERS, []))
                
                # Check if phone number already exists
                if phone_number in current_phones:
                    self._errors["phone_number"] = "Phone number already exists"
                else:
                    # Add the new phone number
                    current_phones.append(phone_number)
                    
                    # Update the config entry
                    new_data = dict(self.config_entry.data)
                    new_data[CONF_PHONE_NUMBERS] = current_phones
                    
                    self.hass.config_entries.async_update_entry(
                        self.config_entry, data=new_data
                    )
                    
                    # Reload the integration to create new entities
                    await self.hass.config_entries.async_reload(self.config_entry.entry_id)
                    
                    return await self.async_step_init()
                    
            except vol.Invalid as err:
                _LOGGER.error("Phone validation error: %s", err)
                self._errors["phone_number"] = "invalid_phone"

        return self.async_show_form(
            step_id="add_phone",
            data_schema=vol.Schema({
                vol.Required("phone_number"): str,
            }),
            errors=self._errors,
        )

    async def async_step_remove_phone(
        self, user_input: Optional[Dict[str, Any]] = None
    ) -> FlowResult:
        """Handle removing a phone number."""
        current_phones = self.config_entry.data.get(CONF_PHONE_NUMBERS, [])
        
        if not current_phones:
            return await self.async_step_init()

        if user_input is not None:
            phone_to_remove = user_input["phone_number"]
            
            # Remove the phone number
            new_phones = [phone for phone in current_phones if phone != phone_to_remove]
            
            # Update the config entry
            new_data = dict(self.config_entry.data)
            new_data[CONF_PHONE_NUMBERS] = new_phones
            
            self.hass.config_entries.async_update_entry(
                self.config_entry, data=new_data
            )
            
            # Reload the integration to remove entities
            await self.hass.config_entries.async_reload(self.config_entry.entry_id)
            
            return await self.async_step_init()

        return self.async_show_form(
            step_id="remove_phone",
            data_schema=vol.Schema({
                vol.Required("phone_number"): vol.In(current_phones),
            }),
        ) 