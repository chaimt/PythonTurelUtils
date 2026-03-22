import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import httpx
import requests
from const import OURRITUAL_CONTEXT_HEADER, TRACEPARENT_HEADER
from error_handling import handle_http_authentication_error
from gcp_files import generate_path
from global_config import GlobalConfig, get_safe_trace_id
from pipeline_utils import (
    OurRitualContext,
    SessionInformation,
    get_authentication_token,
)
from retry_policy import RETRY_POLICY, SkipRetry
from temporal_utils.heartbeat import heartbeat_context
from temporalio import activity

logger = logging.getLogger(__name__)


@asynccontextmanager
async def get_http_client(authentication_url: str, method_name: str, url: Optional[str] = None, data: Optional[dict] = None):
    """
    Global async context manager for httpx.AsyncClient with centralized error handling.

    Catches HTTPStatusError and calls handle_http_authentication_error before re-raising.
    """
    async with httpx.AsyncClient() as client:
        try:
            yield client
        except httpx.HTTPStatusError as http_err:
            response = http_err.response
            handle_http_authentication_error(http_err, response, authentication_url, method_name, url=url, data=data)
            raise http_err
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"🌐 HTTP error occurred: {http_err}")
            raise http_err
        except requests.exceptions.ConnectionError as http_err:
            logger.error("🔌 Error connecting to the server.")
            raise http_err
        except requests.exceptions.Timeout as http_err:
            logger.error("⏰ The request timed out.")
            raise http_err
        except requests.exceptions.RequestException as err:
            logger.error(f"💥 An error occurred: {err}")
            raise err
        except Exception as e:
            logger.error(f"💥 An error occurred: {e}")
            raise e


class SessionNotFoundError(SkipRetry):
    """Exception raised when a session is not found for the given parameters."""

    pass


class ManagementClient:
    def __init__(self, management_api_url: str, authentication_url: str):
        self.management_url = management_api_url
        self.authentication_url = authentication_url

    def get_headers(self):
        if not GlobalConfig().authentication_token or GlobalConfig().authentication_token == "undefined":
            get_authentication_token(self.authentication_url)

        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {GlobalConfig().authentication_token}",
            "x-ritual-source": "session_events_consumer",
            OURRITUAL_CONTEXT_HEADER: OurRitualContext().to_header(),
            TRACEPARENT_HEADER: get_safe_trace_id(),
        }

    async def patch(self, url: str, data: dict) -> httpx.Response:
        async with get_http_client(self.authentication_url, "patch", url=url, data=data) as client:
            response = await client.patch(url, json=data, headers=self.get_headers(), timeout=60)
            response.raise_for_status()
            return response

    async def post(self, url: str, data: dict) -> httpx.Response:
        async with get_http_client(self.authentication_url, "post", url=url, data=data) as client:
            response = await client.post(url, json=data, headers=self.get_headers(), timeout=60)
            response.raise_for_status()
            return response

    @RETRY_POLICY
    async def get_session_info(self, expert_email: str, zoom_meeting_id: str) -> SessionInformation:
        logger.info(f"🔍 Getting session info for {expert_email} and zoom-[{zoom_meeting_id}]")
        if not expert_email or not zoom_meeting_id:
            logger.error("❌ Missing expert email or zoom_meeting_id date in the request")
            raise ValueError("Missing expert email or zoom_meeting_id date in the request")

        url = f"{self.management_url}/v1/sessions/account/"
        data = {"zoom_meeting_id": zoom_meeting_id, "expert_email": expert_email}

        try:
            logger.debug(f"🌐 Getting session info for {expert_email} and zoom-[{zoom_meeting_id}] from {url}")
            response = await self.patch(url, data)
            if response.status_code == 204:
                raise SessionNotFoundError(f"Session not found for {expert_email} and zoom-[{zoom_meeting_id}]")
            else:
                content_dict = json.loads(response.content.decode("utf-8"))[0]
                session_info = SessionInformation.model_validate(content_dict)
                if session_info.error == "Session not found":
                    raise SessionNotFoundError(f"Session not found for {expert_email} and zoom[{zoom_meeting_id}]")
                session_date = datetime.strptime(session_info.session_date, "%Y-%m-%dT%H:%M:%SZ")
                session_date_str = session_date.strftime("%Y/%m/%d/%H/%M")
                session_info.gcp_path = generate_path(session_info.expert.id, session_info.account_id, session_date_str)
                session_info.zoom_meeting_id = str(zoom_meeting_id) if zoom_meeting_id else None
                session_info.insight_sent = content_dict.get("insight_sent")
                session_info.session_id = content_dict.get("session_id")
                return session_info
        except SessionNotFoundError:
            # SessionNotFoundError inherits from SkipRetry, so it will bypass RETRY_POLICY
            # Raise immediately without retry attempts
            raise


class TemporalManagementClient(ManagementClient):
    def __init__(self, management_url: str, authentication_url: str):
        super().__init__(management_url, authentication_url)

    async def patch(self, url: str, data: dict) -> httpx.Response:
        # Use heartbeat context for HTTP call
        async with heartbeat_context(
            interval=10.0,
            details={"operation": "patch", "url": url},
        ):
            activity.heartbeat({"stage": "calling_management_api"})
            return await super().patch(url, data)

    async def post(self, url: str, data: dict) -> httpx.Response:
        # Use heartbeat context for HTTP call
        async with heartbeat_context(
            interval=10.0,
            details={"operation": "post", "url": url},
        ):
            activity.heartbeat({"stage": "calling_management_api"})
            return await super().post(url, data)
