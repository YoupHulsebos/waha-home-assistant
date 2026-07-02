import logging
from typing import Optional, Dict, Any, Union, List
import aiohttp
from aiohttp import ClientSession, ClientTimeout
import json
from .helpers import async_retry
import time
from collections import deque
import asyncio
import traceback
from datetime import datetime, timedelta
from urllib.parse import urljoin

_LOGGER = logging.getLogger(__name__)

class WahaApiError(Exception):
    """Base exception for WAHA API errors."""
    def __init__(self, message: str, status_code: Optional[int] = None, response_text: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text

class WahaConnectionError(WahaApiError):
    """Exception for connection errors."""
    pass

class WahaAuthenticationError(WahaApiError):
    """Exception for authentication errors."""
    pass

class WahaRateLimitError(WahaApiError):
    """Exception for rate limit errors."""
    pass

class WahaApiClient:
    """API client for WAHA (WhatsApp Home Assistant) API."""

    def __init__(
        self, 
        hass: Any,
        base_url: str, 
        api_key: Optional[str], 
        session_name: str, 
        rate_limit: int = 10,
        timeout: int = 30
    ) -> None:
        """Initialize the WAHA API client.
        
        Args:
            hass: Home Assistant instance
            base_url: Base URL of the WAHA API
            api_key: Optional API key for authentication
            session_name: WhatsApp session name
            rate_limit: Maximum number of messages per minute
            timeout: Request timeout in seconds
        """
        self.hass = hass
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.session_name = session_name
        self.timeout = ClientTimeout(total=timeout)
        self._session: Optional[ClientSession] = None
        
        # Rate limiting
        self.rate_limit = rate_limit
        self.message_timestamps: deque = deque(maxlen=rate_limit)
        self._rate_limit_lock = asyncio.Lock()
        self._session_recovery_lock = asyncio.Lock()

    async def _get_session(self) -> ClientSession:
        """Get or create an aiohttp ClientSession."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    async def close(self) -> None:
        """Close the API client session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        if self.api_key:
            headers["X-Api-Key"] = self.api_key
        return headers

    async def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
        retry_attempts: int = 3,
        retry_delay: float = 1.0,
    ) -> Any:
        """Make an API request.
        
        Args:
            method: HTTP method
            endpoint: API endpoint (without leading slash)
            data: Request body data
            params: URL parameters
            timeout: Request timeout in seconds
            retry_attempts: Number of retry attempts for transient failures
            retry_delay: Delay between retries in seconds
            
        Returns:
            Any: Response data
            
        Raises:
            WahaConnectionError: On connection errors
            WahaAuthenticationError: On authentication failure
            WahaRateLimitError: On rate limit exceeded
            WahaApiError: On other API errors
        """
        # Remove any leading slashes but keep the 'api/' prefix
        endpoint = endpoint.lstrip('/')
        url = urljoin(self.base_url + '/', endpoint)
        session_recovery_attempted = False
        skip_session_recovery = endpoint.startswith("api/sessions")
        
        session = await self._get_session()
        
        if timeout and timeout != self.timeout.total:
            timeout_obj = ClientTimeout(total=timeout)
        else:
            timeout_obj = self.timeout

        async def _request() -> Any:
            try:
                async with session.request(
                    method=method,
                    url=url,
                    json=data,
                    params=params,
                    headers=self._get_headers(),
                    timeout=timeout_obj
                ) as resp:
                    if resp.status == 401:
                        raise WahaAuthenticationError("Authentication failed", resp.status)
                    elif resp.status == 429:
                        raise WahaRateLimitError("Rate limit exceeded", resp.status)
                    elif 500 <= resp.status < 600:
                        text = await resp.text()
                        raise WahaConnectionError(
                            f"Server error: {resp.status}",
                            resp.status,
                            text,
                        )
                    elif resp.status not in [200, 201]:
                        text = await resp.text()
                        raise WahaApiError(
                            f"API request failed: {resp.status}",
                            resp.status,
                            text
                        )

                    try:
                        return await resp.json()
                    except json.JSONDecodeError as exc:
                        text = await resp.text()
                        raise WahaApiError(
                            f"Invalid JSON response: {exc}",
                            resp.status,
                            text
                        )

            except asyncio.TimeoutError as exc:
                raise WahaConnectionError(f"Request timed out: {exc}")
            except aiohttp.ClientError as exc:
                raise WahaConnectionError(f"Connection error: {exc}")

        async def _request_with_recovery() -> Any:
            nonlocal session_recovery_attempted
            try:
                return await _request()
            except WahaApiError as exc:
                if session_recovery_attempted or skip_session_recovery:
                    raise

                session_recovery_attempted = True
                _LOGGER.warning(
                    "WAHA API error on %s %s: %s (status: %s). Attempting session recovery...",
                    method,
                    endpoint,
                    exc,
                    exc.status_code,
                )

                async with self._session_recovery_lock:
                    if not await self.ensure_session_active():
                        _LOGGER.error(
                            "Failed to recover session after error on %s %s. "
                            "Manual intervention may be required.",
                            method,
                            endpoint,
                        )
                        raise

                _LOGGER.info("Session recovered successfully. Retrying request %s %s...", method, endpoint)
                # Convert successful recovery into a retryable error for async_retry.
                raise WahaConnectionError(
                    f"Session recovered; retrying request {method} {endpoint}"
                )

        return await async_retry(
            _request_with_recovery,
            attempts=retry_attempts,
            delay=retry_delay,
            exceptions=(WahaConnectionError, WahaRateLimitError),
        )

    async def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect the rate limit."""
        async with self._rate_limit_lock:
            now = time.time()
            # Remove timestamps older than 60 seconds
            while self.message_timestamps and now - self.message_timestamps[0] > 60:
                self.message_timestamps.popleft()
            
            # If we've reached the rate limit, wait until we can send again
            if len(self.message_timestamps) >= self.rate_limit:
                wait_time = 60 - (now - self.message_timestamps[0])
                if wait_time > 0:
                    _LOGGER.debug(f"Rate limit reached, waiting {wait_time:.2f} seconds")
                    await asyncio.sleep(wait_time)
            
            # Add current timestamp
            self.message_timestamps.append(time.time())

    async def test_connection(self) -> bool:
        """Test the connection to the WAHA server.
        
        Returns:
            bool: True if connection is successful
        """
        endpoints = ["api/server/version", "api/version"]

        for endpoint in endpoints:
            try:
                response = await self._make_request("GET", endpoint, timeout=10)
                if isinstance(response, dict) and "version" in response:
                    _LOGGER.debug(
                        "WAHA connection test successful via %s. Version: %s",
                        endpoint,
                        response.get("version"),
                    )
                    return True
                _LOGGER.warning(
                    "WAHA connection test got unexpected response via %s: %s",
                    endpoint,
                    type(response).__name__,
                )
            except WahaApiError as exc:
                _LOGGER.warning(
                    "WAHA connection test failed via %s: %s (status: %s)",
                    endpoint,
                    exc,
                    exc.status_code,
                )
            except Exception as exc:
                _LOGGER.warning(
                    "WAHA connection test error via %s: %s\n%s",
                    endpoint,
                    exc,
                    traceback.format_exc(),
                )

        _LOGGER.error("WAHA connection test failed for all known version endpoints")
        return False

    async def send_message(
        self, 
        chat_id: str, 
        message: str,
        retry_attempts: int = 3,
        retry_delay: float = 1.0
    ) -> bool:
        """Send a WhatsApp message.
        
        Args:
            chat_id: WhatsApp chat ID (phone number with @c.us suffix or phone number)
            message: Message text
            retry_attempts: Number of retry attempts
            retry_delay: Delay between retries in seconds
        
        Returns:
            bool: True if message was sent successfully
        """
        await self._wait_for_rate_limit()
        
        # Ensure chat_id has proper format for WhatsApp
        if not chat_id.endswith("@c.us") and not chat_id.endswith("@g.us"):
            # If it's just a phone number, format it properly
            clean_number = chat_id.lstrip('+').replace(' ', '').replace('-', '')
            chat_id = f"{clean_number}@c.us"

        payload = {
            "session": self.session_name,
            "chatId": chat_id,
            "text": message,
        }

        try:
            # Use the correct WAHA endpoint format: /api/sendText
            response = await self._make_request(
                "POST",
                "api/sendText",
                data=payload,
                timeout=15,
                retry_attempts=retry_attempts,
                retry_delay=retry_delay,
            )
            _LOGGER.info("Message sent successfully to %s, message ID: %s", chat_id, response.get("id", "unknown"))
            return True
        except WahaApiError as exc:
            _LOGGER.error(
                "WAHA API error sending message to %s: %s (status: %s, response: %s)",
                chat_id,
                exc,
                exc.status_code,
                exc.response_text,
            )
            return False
        except Exception as exc:
            _LOGGER.error("Error sending message to %s: %s", chat_id, exc)
            return False

    async def send_location(
        self,
        chat_id: str,
        latitude: float,
        longitude: float,
        title: str,
    ) -> bool:
        """Send a WhatsApp location message.

        Args:
            chat_id: WhatsApp chat ID (phone number with @c.us suffix or phone number)
            latitude: Latitude of the location
            longitude: Longitude of the location
            title: Title/name of the location

        Returns:
            bool: True if message was sent successfully
        """
        await self._wait_for_rate_limit()

        if not chat_id.endswith("@c.us") and not chat_id.endswith("@g.us"):
            clean_number = chat_id.lstrip("+").replace(" ", "").replace("-", "")
            chat_id = f"{clean_number}@c.us"

        payload = {
            "session": self.session_name,
            "chatId": chat_id,
            "latitude": latitude,
            "longitude": longitude,
            "title": title,
        }

        try:
            response = await self._make_request("POST", "api/sendLocation", data=payload, timeout=15)
            _LOGGER.info(
                "Location sent successfully to %s, message ID: %s",
                chat_id,
                response.get("id", "unknown"),
            )
            return True
        except Exception as exc:
            _LOGGER.error("Error sending location to %s: %s", chat_id, exc)
            return False

    async def get_qr_code(self) -> Optional[str]:
        """Get the QR code for WhatsApp Web authentication.
        
        Returns:
            Optional[str]: QR code data or None if not available
        """
        try:
            # Use the correct WAHA endpoint format: /api/sessions/qr with session parameter
            response = await self._make_request(
                "GET", 
                "api/sessions/qr",
                params={"session": self.session_name}
            )
            return response.get("qr")
        except WahaApiError as exc:
            _LOGGER.error("Failed to get QR code: %s", exc)
            return None

    async def get_session_status(self) -> Optional[str]:
        """Get the current WhatsApp session status.
        
        Returns:
            Optional[str]: Session status or None if request failed
        """
        try:
            # Use the correct WAHA endpoint format: /api/sessions/{session}
            endpoint = f"api/sessions/{self.session_name}"
            response = await self._make_request("GET", endpoint)
            return response.get("status")
        except WahaApiError as exc:
            _LOGGER.error("Failed to get session status: %s", exc)
            return None

    async def logout(self) -> bool:
        """Logout from the WhatsApp session.
        
        Returns:
            bool: True if logout was successful
        """
        try:
            # Use the correct WAHA endpoint format: /api/sessions/logout
            await self._make_request(
                "POST", 
                "api/sessions/logout",
                data={"session": self.session_name}
            )
            return True
        except WahaApiError as exc:
            _LOGGER.error("Failed to logout: %s", exc)
            return False

    async def stop_session(self) -> bool:
        """Stop the WhatsApp session.

        Returns:
            bool: True if session stop was successful
        """
        try:
            endpoint = f"api/sessions/{self.session_name}/stop"
            await self._make_request("POST", endpoint)
            _LOGGER.info("Session stopped successfully: %s", self.session_name)
            return True
        except WahaApiError as exc:
            _LOGGER.error("Failed to stop session %s: %s (status: %s)",
                         self.session_name, exc, exc.status_code)
            return False

    async def start_session(self) -> bool:
        """Start the WhatsApp session.
        
        Returns:
            bool: True if session start was successful
        """
        try:
            if not await self.stop_session():
                _LOGGER.warning(
                    "Could not stop session %s before start; continuing anyway",
                    self.session_name,
                )

            endpoint = f"api/sessions/{self.session_name}/start"
            await self._make_request("POST", endpoint)
            _LOGGER.info("Session started successfully: %s", self.session_name)
            return True
        except WahaApiError as exc:
            _LOGGER.error("Failed to start session %s: %s (status: %s)", 
                         self.session_name, exc, exc.status_code)
            return False

    async def wait_for_session_working(
        self, 
        timeout: int = 120, 
        poll_interval: float = 3.0
    ) -> bool:
        """Wait for the session to reach WORKING status.
        
        Args:
            timeout: Maximum time to wait in seconds
            poll_interval: How often to check status in seconds
        
        Returns:
            bool: True if session reached WORKING status, False if timeout or unrecoverable status
        """
        start_time = time.time()
        
        while True:
            elapsed = time.time() - start_time
            
            if elapsed > timeout:
                _LOGGER.error(
                    "Timeout waiting for session %s to reach WORKING status (waited %d seconds)",
                    self.session_name, timeout
                )
                return False
            
            try:
                status = await self.get_session_status()
                
                if status == "WORKING":
                    _LOGGER.info("Session %s is now WORKING", self.session_name)
                    return True
                elif status == "STARTING":
                    _LOGGER.debug(
                        "Session %s is starting (elapsed: %.1f seconds)",
                        self.session_name, elapsed
                    )
                    await asyncio.sleep(poll_interval)
                elif status in ["SCAN_QR_CODE", "FAILED"]:
                    _LOGGER.error(
                        "Session %s is in unrecoverable state: %s. "
                        "Manual intervention required (scan QR code or check logs).",
                        self.session_name, status
                    )
                    return False
                else:
                    _LOGGER.warning(
                        "Session %s has unexpected status: %s (elapsed: %.1f seconds)",
                        self.session_name, status, elapsed
                    )
                    await asyncio.sleep(poll_interval)
                    
            except Exception as exc:
                _LOGGER.warning(
                    "Error checking session status for %s: %s (elapsed: %.1f seconds). Retrying...",
                    self.session_name, exc, elapsed
                )
                await asyncio.sleep(poll_interval)

    async def ensure_session_active(self) -> bool:
        """Ensure the session is in WORKING status.
        
        If the session is STOPPED, start it and wait for WORKING status.
        If the session is STARTING, wait for WORKING status.
        If the session is in SCAN_QR_CODE or FAILED, return False (manual intervention needed).
        
        Returns:
            bool: True if session is (or was recovered to) WORKING status, False otherwise
        """
        try:
            status = await self.get_session_status()
            
            if status == "WORKING":
                _LOGGER.debug("Session %s is already WORKING", self.session_name)
                return True
            
            elif status == "STOPPED":
                _LOGGER.info("Session %s is STOPPED. Attempting to start...", self.session_name)
                if await self.start_session():
                    _LOGGER.info("Session %s started. Waiting for WORKING status...", self.session_name)
                    return await self.wait_for_session_working()
                else:
                    _LOGGER.error("Failed to start session %s", self.session_name)
                    return False
            
            elif status == "STARTING":
                _LOGGER.info("Session %s is STARTING. Waiting for WORKING status...", self.session_name)
                return await self.wait_for_session_working()
            
            elif status in ["SCAN_QR_CODE", "FAILED"]:
                _LOGGER.error(
                    "Session %s is in unrecoverable state: %s. "
                    "Manual intervention required (scan QR code or check server logs).",
                    self.session_name, status
                )
                return False
            
            else:
                _LOGGER.warning(
                    "Session %s has unknown status: %s. Attempting to wait for WORKING...",
                    self.session_name, status
                )
                return await self.wait_for_session_working(timeout=10)
                
        except Exception as exc:
            _LOGGER.error(
                "Unexpected error while ensuring session %s is active: %s\n%s",
                self.session_name, exc, traceback.format_exc()
            )
            return False

    async def start_typing(self, chat_id: str) -> bool:
        """Send a typing indicator to a chat.
        
        Args:
            chat_id: WhatsApp chat ID (phone number with @c.us suffix or phone number)
        
        Returns:
            bool: True if typing indicator was sent successfully
        """
        # Ensure chat_id has proper format for WhatsApp
        if not chat_id.endswith("@c.us") and not chat_id.endswith("@g.us"):
            # If it's just a phone number, format it properly
            clean_number = chat_id.lstrip('+').replace(' ', '').replace('-', '')
            chat_id = f"{clean_number}@c.us"
        
        payload = {
            "session": self.session_name,
            "chatId": chat_id,
        }
        try:
            await self._make_request("POST", "api/startTyping", data=payload, timeout=10)
            _LOGGER.debug("Typing indicator started for %s", chat_id)
            return True
        except WahaApiError as exc:
            _LOGGER.error("WAHA API error starting typing for %s: %s (status: %s)", 
                         chat_id, exc, exc.status_code)
            return False
        except Exception as exc:
            _LOGGER.error("Error starting typing for %s: %s", chat_id, exc)
            return False

    async def stop_typing(self, chat_id: str) -> bool:
        """Stop sending a typing indicator to a chat.
        
        Args:
            chat_id: WhatsApp chat ID (phone number with @c.us suffix or phone number)
        
        Returns:
            bool: True if typing indicator was stopped successfully
        """
        # Ensure chat_id has proper format for WhatsApp
        if not chat_id.endswith("@c.us") and not chat_id.endswith("@g.us"):
            # If it's just a phone number, format it properly
            clean_number = chat_id.lstrip('+').replace(' ', '').replace('-', '')
            chat_id = f"{clean_number}@c.us"
        
        payload = {
            "session": self.session_name,
            "chatId": chat_id,
        }
        try:
            await self._make_request("POST", "api/stopTyping", data=payload, timeout=10)
            _LOGGER.debug("Typing indicator stopped for %s", chat_id)
            return True
        except WahaApiError as exc:
            _LOGGER.error("WAHA API error stopping typing for %s: %s (status: %s)", 
                         chat_id, exc, exc.status_code)
            return False
        except Exception as exc:
            _LOGGER.error("Error stopping typing for %s: %s", chat_id, exc)
            return False 