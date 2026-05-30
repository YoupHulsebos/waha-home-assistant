"""The WAHA WhatsApp integration."""
import logging
import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.typing import ConfigType
from homeassistant.exceptions import HomeAssistantError
from homeassistant.const import Platform

from .const import (
    DOMAIN,
    CONF_BASE_URL,
    CONF_API_KEY,
    CONF_PHONE_NUMBERS,
    CONF_SESSION_NAME,
    DEFAULT_SESSION_NAME,
)
from .api_client import WahaApiClient

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.BUTTON]

# Service schemas
SEND_MESSAGE_SERVICE_SCHEMA = vol.Schema(
    {
        vol.Required("phone_number"): cv.string,
        vol.Required("message"): cv.template,
    },
    extra=vol.ALLOW_EXTRA,
)

START_TYPING_SERVICE_SCHEMA = vol.Schema(
    {
        vol.Required("phone_number"): cv.string,
    },
    extra=vol.ALLOW_EXTRA,
)

STOP_TYPING_SERVICE_SCHEMA = vol.Schema(
    {
        vol.Required("phone_number"): cv.string,
    },
    extra=vol.ALLOW_EXTRA,
)

SEND_LOCATION_SERVICE_SCHEMA = vol.Schema(
    {
        vol.Required("phone_number"): cv.string,
        vol.Required("latitude"): vol.Coerce(float),
        vol.Required("longitude"): vol.Coerce(float),
        vol.Required("title"): cv.string,
    },
    extra=vol.ALLOW_EXTRA,
)

SERVICE_SCHEMAS = {
    "send_message": SEND_MESSAGE_SERVICE_SCHEMA,
    "start_typing": START_TYPING_SERVICE_SCHEMA,
    "stop_typing": STOP_TYPING_SERVICE_SCHEMA,
}

async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the WAHA integration."""
    hass.data.setdefault(DOMAIN, {})
    return True

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up WAHA from a config entry."""
    _LOGGER.info("Setting up WAHA integration")
    
    # Initialize API client
    api_client = WahaApiClient(
        hass=hass,
        base_url=entry.data[CONF_BASE_URL],
        api_key=entry.data.get(CONF_API_KEY),
        session_name=entry.data.get(CONF_SESSION_NAME, DEFAULT_SESSION_NAME),
    )
    
    # Store entry data and API client
    hass.data[DOMAIN][entry.entry_id] = {
        "entry": entry,
        "api_client": api_client,
        "base_url": entry.data[CONF_BASE_URL],
        "api_key": entry.data.get(CONF_API_KEY),
        "session_name": entry.data.get(CONF_SESSION_NAME, DEFAULT_SESSION_NAME),
        "phone_numbers": entry.data.get(CONF_PHONE_NUMBERS, []),
    }

    # Register services
    async def handle_send_message(call: ServiceCall) -> None:
        """Handle the send_message service call."""
        phone_number = call.data.get("phone_number")
        message = call.data.get("message")
        
        if not phone_number or not message:
            raise HomeAssistantError("Both phone_number and message are required")
        
        # Render template if needed
        if hasattr(message, 'async_render'):
            message = message.async_render()
        
        _LOGGER.info("WAHA: Sending message to %s: %s", phone_number, message)
        
        try:
            result = await api_client.send_message(chat_id=phone_number, message=str(message))
            _LOGGER.info("WAHA: Message sent successfully: %s", result)
        except Exception as err:
            _LOGGER.error("WAHA: Failed to send message: %s", err)
            raise HomeAssistantError(f"Failed to send message: {err}") from err
    
    # Register service
    if not hass.services.has_service(DOMAIN, "send_message"):
        hass.services.async_register(
            DOMAIN,
            "send_message",
            handle_send_message,
            schema=SEND_MESSAGE_SERVICE_SCHEMA,
        )
        _LOGGER.info("WAHA: Service registered: waha.send_message")
    
    # Register start_typing service
    async def handle_start_typing(call: ServiceCall) -> None:
        """Handle the start_typing service call."""
        phone_number = call.data.get("phone_number")
        
        if not phone_number:
            raise HomeAssistantError("phone_number is required")
        
        _LOGGER.info("WAHA: Starting typing indicator for %s", phone_number)
        
        try:
            result = await api_client.start_typing(chat_id=phone_number)
            if result:
                _LOGGER.debug("WAHA: Typing indicator started for %s", phone_number)
            else:
                _LOGGER.error("WAHA: Failed to start typing indicator for %s", phone_number)
        except Exception as err:
            _LOGGER.error("WAHA: Error starting typing indicator for %s: %s", phone_number, err)
            raise HomeAssistantError(f"Failed to start typing: {err}") from err
    
    if not hass.services.has_service(DOMAIN, "start_typing"):
        hass.services.async_register(
            DOMAIN,
            "start_typing",
            handle_start_typing,
            schema=START_TYPING_SERVICE_SCHEMA,
        )
        _LOGGER.info("WAHA: Service registered: waha.start_typing")
    
    # Register stop_typing service
    async def handle_stop_typing(call: ServiceCall) -> None:
        """Handle the stop_typing service call."""
        phone_number = call.data.get("phone_number")
        
        if not phone_number:
            raise HomeAssistantError("phone_number is required")
        
        _LOGGER.info("WAHA: Stopping typing indicator for %s", phone_number)
        
        try:
            result = await api_client.stop_typing(chat_id=phone_number)
            if result:
                _LOGGER.debug("WAHA: Typing indicator stopped for %s", phone_number)
            else:
                _LOGGER.error("WAHA: Failed to stop typing indicator for %s", phone_number)
        except Exception as err:
            _LOGGER.error("WAHA: Error stopping typing indicator for %s: %s", phone_number, err)
            raise HomeAssistantError(f"Failed to stop typing: {err}") from err
    
    if not hass.services.has_service(DOMAIN, "stop_typing"):
        hass.services.async_register(
            DOMAIN,
            "stop_typing",
            handle_stop_typing,
            schema=STOP_TYPING_SERVICE_SCHEMA,
        )
        _LOGGER.info("WAHA: Service registered: waha.stop_typing")

    # Register send_location service
    async def handle_send_location(call: ServiceCall) -> None:
        """Handle the send_location service call."""
        phone_number = call.data.get("phone_number")
        latitude = call.data.get("latitude")
        longitude = call.data.get("longitude")
        title = call.data.get("title")

        if not phone_number:
            raise HomeAssistantError("phone_number is required")

        _LOGGER.info(
            "WAHA: Sending location to %s: %s, %s (%s)",
            phone_number, latitude, longitude, title,
        )

        try:
            result = await api_client.send_location(
                chat_id=phone_number,
                latitude=latitude,
                longitude=longitude,
                title=title,
            )
            if result:
                _LOGGER.info("WAHA: Location sent successfully to %s", phone_number)
            else:
                raise HomeAssistantError(f"Failed to send location to {phone_number}")
        except HomeAssistantError:
            raise
        except Exception as err:
            _LOGGER.error("WAHA: Error sending location to %s: %s", phone_number, err)
            raise HomeAssistantError(f"Failed to send location: {err}") from err

    if not hass.services.has_service(DOMAIN, "send_location"):
        hass.services.async_register(
            DOMAIN,
            "send_location",
            handle_send_location,
            schema=SEND_LOCATION_SERVICE_SCHEMA,
        )
        _LOGGER.info("WAHA: Service registered: waha.send_location")

    # Set up platforms  
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    
    # Set up notification services for each phone number
    phone_numbers = entry.data.get(CONF_PHONE_NUMBERS, [])
    for phone_number in phone_numbers:
        # Clean phone number for service name (remove + and spaces)
        clean_number = phone_number.replace("+", "").replace(" ", "").replace("-", "")
        service_name = f"waha_{clean_number}"
        
        # Create notification service handler with proper closure
        def create_notification_handler(target_phone):
            """Create a notification handler for a specific phone number."""
            async def send_notification_handler(call):
                """Handle notification service call."""
                message = call.data.get("message", "")
                try:
                    # Convert template if needed
                    if hasattr(message, 'async_render'):
                        message = message.async_render()
                    
                    message_text = str(message).strip()
                    if not message_text:
                        _LOGGER.error("Cannot send empty message to %s", target_phone)
                        return
                    
                    _LOGGER.info("Sending notification to %s: %s", target_phone, message_text)
                    
                    result = await api_client.send_message(
                        chat_id=target_phone,
                        message=message_text
                    )
                    
                    if result:
                        _LOGGER.info("Notification sent successfully to %s", target_phone)
                    else:
                        _LOGGER.error("Failed to send notification to %s", target_phone)
                        
                except Exception as err:
                    _LOGGER.error("Error sending notification to %s: %s", target_phone, err)
            return send_notification_handler
        
        # Register the notification service with proper schema
        hass.services.async_register(
            "notify",
            service_name,
            create_notification_handler(phone_number),
            schema=vol.Schema({
                vol.Required("message"): cv.template,
                vol.Optional("title"): cv.template,
            }),
        )
        
        _LOGGER.info("Registered notification service: notify.%s for %s", service_name, phone_number)
    
    _LOGGER.info("WAHA integration setup completed")
    return True

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    _LOGGER.info("Unloading WAHA integration")
    
    # Unload platforms
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    
    if unload_ok:
        # Remove from hass.data
        hass.data[DOMAIN].pop(entry.entry_id, None)
        
        # Remove services if this was the last entry
        if not hass.data[DOMAIN]:
            if hass.services.has_service(DOMAIN, "send_message"):
                hass.services.async_remove(DOMAIN, "send_message")
                _LOGGER.info("WAHA: Removed send_message service")
            if hass.services.has_service(DOMAIN, "start_typing"):
                hass.services.async_remove(DOMAIN, "start_typing")
                _LOGGER.info("WAHA: Removed start_typing service")
            if hass.services.has_service(DOMAIN, "stop_typing"):
                hass.services.async_remove(DOMAIN, "stop_typing")
                _LOGGER.info("WAHA: Removed stop_typing service")
            if hass.services.has_service(DOMAIN, "send_location"):
                hass.services.async_remove(DOMAIN, "send_location")
                _LOGGER.info("WAHA: Removed send_location service")

    return unload_ok 