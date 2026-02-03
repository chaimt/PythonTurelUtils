import asyncio
import functools
import json
import logging
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import httpx
from const import OURRITUAL_CONTEXT_HEADER
from gcp_files import download_string, upload_string
from global_config import GlobalConfig
from log_helper import OurRitualContext
from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import Status, StatusCode
from pydantic import BaseModel, ConfigDict, Field
from retry_policy import RETRY_POLICY

logger = logging.getLogger(__name__)


@RETRY_POLICY
def get_authentication_token(authentication_url):
    logger.debug(f"🔑 Getting authentication token: {authentication_url}")
    response = httpx.get(authentication_url, timeout=10)
    response.raise_for_status()
    GlobalConfig().authentication_token = response.content.decode("utf-8")
    return GlobalConfig().authentication_token


class ExpertInformation(BaseModel):
    id: Optional[int] = None
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class MemberInformation(BaseModel):
    id: Optional[int] = None
    uuid: Optional[str] = None
    age: Optional[int] = None
    gender: Optional[str] = None
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class WebhookType(str, Enum):
    FATHOM = "fathom"
    ZOOM = "zoom"


class ParticipantInformation(BaseModel):
    join_time: Optional[str] = None
    leave_time: Optional[str] = None


class FileInformation(BaseModel):
    id: Optional[str] = None
    email: Optional[str] = None
    first_name: Optional[str] = None
    file_name: Optional[str] = None
    file_type: Optional[str] = None
    file_url: Optional[str] = None
    participant_information: List[ParticipantInformation] = Field(default_factory=list)
    match_score: Optional[int] = None


class UrlInformation(BaseModel):
    webhook_type: Optional[WebhookType] = None
    host_audio_url: Optional[str] = None
    host_video_url: Optional[str] = None
    host_video_url_link: Optional[str] = None
    end_session: Optional[str] = None
    participant_urls: List[FileInformation] = Field(default_factory=list)
    extra_urls: List[FileInformation] = Field(default_factory=list)
    transcription_url: Optional[str] = None
    total_speakers: Optional[int] = None
    speakers_transcription_url: Optional[str] = None
    transcription_engine: Optional[str] = None


class RecordingSpan(BaseModel):
    message_type: Optional[str] = None
    recording_date: Optional[str] = None
    recording_language: Optional[str] = None
    number_of_speakers: Optional[int] = None
    found_speakers: Optional[List[str]] = Field(default_factory=list)
    url_info: UrlInformation = Field(default_factory=UrlInformation)
    speech_identification_prompt_lables: Optional[List[str]] = None
    speech_identification_prompt_version: Optional[int] = None
    empty_transcription: Optional[bool] = None


class PromptInsightInformation(BaseModel):
    prompt_template_name: str
    prompt_template_label: Optional[List[str]] = None
    prompt_template_version: Optional[int] = None
    input_params: Optional[Dict] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None


class InsightInformation(BaseModel):
    prompt_insight_information: List[PromptInsightInformation] = Field(default_factory=list)
    error: Optional[str] = None


class SessionSummaryInformation(BaseModel):
    prompt_id: Optional[int] = None
    prompt_type: Optional[str] = None
    file_name: Optional[str] = None
    session_summary: Optional[str] = None
    session_summary_url: Optional[str] = None
    status: Optional[str] = None


class SessionInformation(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "session_id": 123,
                    "account_id": 456,
                    "account_uuid": "550e8400-e29b-41d4-a716-446655440000",
                    "expert": {"id": 789, "email": "expert@example.com", "first_name": "John"},
                    "session_date": "2024-01-01",
                    "member_information": [{"id": 101, "uuid": "660e8400-e29b-41d4-a716-446655440001", "age": 30, "gender": "M", "email": "member@example.com", "first_name": "Jane"}],
                    "gcp_path": "recordings/2024/01/01",
                    "recording_language": "en-US",
                    "transcription_engine": "whisper",
                    "number_of_speakers": 2,
                    "url_info": {
                        "video_url": "https://storage.example.com/video.mp4",
                        "transcription_url": "https://storage.example.com/transcript.txt",
                        "speakers_transcription_url": "https://storage.example.com/speakers.txt",
                        "transcription_engine": "whisper",
                    },
                }
            ]
        }
    )
    id: Optional[int] = None
    action: Optional[str] = None
    error: Optional[str] = None
    session_id: Optional[str] = None
    account_id: Optional[int] = None
    account_uuid: Optional[str] = None
    session_type: Optional[str] = None
    insight_sent: Optional[bool] = None
    expert: ExpertInformation = Field(default_factory=ExpertInformation)
    session_date: Optional[str] = None
    zoom_meeting_id: Optional[str] = None
    member_information: List[MemberInformation] = Field(default_factory=list)
    gcp_path: Optional[str] = None
    welcome_gcp_path: Optional[str] = None
    transcription_engine: Optional[str] = None
    replay: Optional[bool] = None
    recording_spans: List[RecordingSpan] = Field(default_factory=list)
    speakers_transcription_url: Optional[str] = None
    transcription_summary_url: Optional[str] = None
    insight_information: InsightInformation = Field(default_factory=InsightInformation)
    session_summaries: List[SessionSummaryInformation] = Field(default_factory=list)
    trace_id: Optional[str] = None

    def get_identifier(self):
        return f"[A:{self.account_id} E:{self.expert.id}]-{self.session_date} [{self.get_zoom_id()}]"

    def get_zoom_id(self):
        return str(self.zoom_meeting_id) if self.zoom_meeting_id and str(self.zoom_meeting_id) != "None" else None

    def get_media_path(self, recording_span: RecordingSpan, file_name: str, file_extension: str):
        if "&" in file_extension:
            file_extension = file_extension.split("&")[0]
        return f"{self.gcp_path}/{recording_span.recording_date}/{self.expert.id}_{file_name}.{file_extension}"

    def get_data_path(self):
        return f"{self.gcp_path}/data"

    def get_last_recording_span(self) -> RecordingSpan:
        return self.recording_spans[-1]

    def save_to_gcp(self, service_name: Optional[str] = None):
        try:
            service_name = GlobalConfig().settings.service_name if service_name is None else service_name
            path = self.gcp_path
            gcp_session_data_file = f"{path}/{self.expert.id}_{service_name}_session_data.json"

            # Create blob and upload the transcription
            upload_string(json.dumps(self.model_dump()), gcp_session_data_file)
            return gcp_session_data_file
        except Exception as e:
            logger.error(f"❌ Failed to upload session data: {self.get_identifier()}: {str(e)}")
            raise

    @staticmethod
    def read_from_gcp(gcp_path: str, expert_id: int, service_name: Optional[str] = None) -> Optional["SessionInformation"]:
        try:
            if service_name is None:
                service_name = GlobalConfig().settings.service_name
            gcp_session_data_file = f"{gcp_path}/{expert_id}_{service_name}_session_data.json"

            # Download and parse the session data
            json_str = download_string(gcp_session_data_file)
            if json_str:
                return SessionInformation.model_validate_json(json_str)
            return None
        except Exception as e:
            logger.error(f"❌ Failed to read session data from {gcp_session_data_file}: {str(e)}")
            return None

    def add_to_context(self):
        OurRitualContext().set_custom_info("ourritual/session/id", self.session_id)
        OurRitualContext().set_custom_info("ourritual/session/expert/id", self.expert.id)
        OurRitualContext().set_custom_info("ourritual/session/expert/email", self.expert.email)
        OurRitualContext().set_custom_info("ourritual/session/zoom_meeting_id", self.zoom_meeting_id)
        for pos, member in enumerate(self.member_information or []):
            OurRitualContext().set_custom_info(f"ourritual/members/{pos}/id", member.id)
            OurRitualContext().set_custom_info(f"ourritual/members/{pos}/email", member.email)


def extract_trace_context_from_message(message):
    """Extract trace context from Pub/Sub message attributes and set as current context.

    Args:
        message: Pub/Sub message object with attributes

    Returns:
        bool: True if trace context was found and set, False otherwise
    """
    try:
        if not message.attributes:
            return False

        # Look for trace context in message attributes
        trace_id = message.attributes.get("trace_id") or message.attributes.get("X-Cloud-Trace-Context")
        span_id = message.attributes.get("span_id")

        if trace_id:
            # Parse trace context and set as current
            # The trace_id format might be: "projects/PROJECT_ID/traces/TRACE_ID"
            if "/" in trace_id:
                trace_id = trace_id.split("/")[-1]

            context = extract(message.attributes)

            # Create a span with the extracted context
            with GlobalConfig().tracer.start_as_current_span(
                "extract_trace_context_from_message",
                context=context,
                kind=SpanKind.CONSUMER,
                attributes={
                    "messaging.system": "pubsub",
                    "messaging.operation": "process",
                    "messaging.message_id": message.message_id,
                    "service.name": GlobalConfig().settings.service_name,
                },
            ) as span:
                if span.get_span_context().trace_id != int(trace_id, 16):
                    logger.warning(f"⚠️ Trace context mismatch: {span.get_span_context().trace_id} != {trace_id}")
                else:
                    logger.debug(f"🔍 Trace context extracted and set: trace_id={trace_id}, span_id={span_id}")
                return True

    except Exception as e:
        logger.warning(f"⚠️ Failed to extract trace context from message: {e}")

    return False


def create_message_with_trace_context(data, attributes=None):
    """Create a message with trace context attributes for publishing.

    Args:
        data: Message data to publish
        attributes: Optional additional attributes

    Returns:
        tuple: (data, attributes_with_trace)
    """
    try:
        current_span = trace.get_current_span()
        span_context = current_span.get_span_context()

        # Always try to add trace context if we have a valid trace_id
        if current_span and span_context.trace_id != 0:
            # Add trace context to attributes
            if attributes is None:
                attributes = {}

            inject(attributes)

            attributes.update(
                {
                    "trace_id": format(span_context.trace_id, "032x"),
                    "span_id": format(span_context.span_id, "016x"),
                    "trace_sampled": str(span_context.trace_flags & 0x01),
                    "trace_valid": str(span_context.is_valid),
                }
            )

            logger.debug(f"🔗 Added trace context to message: trace_id={attributes['trace_id']}, sampled={attributes['trace_sampled']}")

    except Exception as e:
        logger.warning(f"⚠️ Failed to add trace context to message: {e}")

    return data, attributes or {}


def wrap_message_callback(callback_func):
    """Wrapper function to extract trace context from Pub/Sub messages and set it as current context.

    Args:
        callback_func: The original message callback function

    Returns:
        function: Wrapped callback function with trace context handling

    Example:
        def my_message_callback(message):
            # This function will automatically have trace context set
            # if it was present in the message attributes
            logger.debug(f"📨 Processing message: {message.message_id}")
            # Your message processing logic here
            return True

        # The callback is automatically wrapped when passed to setup_incoming_subscriber
        setup_incoming_subscriber(settings, my_message_callback)
    """

    def wrapped_callback(message):
        logger.debug(f"🔍 Extracting trace context from message: {message.attributes}")
        # Extract trace context from message and set as current
        context = extract(message.attributes)
        ritual_context = message.attributes.get(OURRITUAL_CONTEXT_HEADER, "")
        if ritual_context:
            OurRitualContext().from_header(ritual_context)
        if GlobalConfig().tracer:
            with GlobalConfig().tracer.start_as_current_span(
                f"{GlobalConfig().settings.service_name} - callback",
                context=context,
                kind=SpanKind.CONSUMER,
                attributes={
                    "messaging.system": "pubsub",
                    "messaging.operation": "process",
                    "messaging.message_id": message.message_id,
                    "service.name": GlobalConfig().settings.service_name,
                },
            ) as span:
                try:
                    return callback_func(message)
                except Exception as e:
                    logger.error(f"❌ Error in wrapped message callback: {e}")
                    # Record error in the span while it's still active
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise
        else:
            return callback_func(message)

    return wrapped_callback


def str_to_bool(s):
    return s.strip().lower() in ("true", "1", "yes", "y")


def log_execution_time(func: Callable) -> Callable:
    """
    Async-aware decorator that logs the execution time of a function.
    Works with both sync and async functions.
    """

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.debug(f"✅ Async Function {func.__name__} completed successfully in {execution_time:.4f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"❌ Async Function {func.__name__} failed after {execution_time:.4f} seconds with error: {str(e)}")
            raise

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.debug(f"✅ Sync Function {func.__name__} completed successfully in {execution_time:.4f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"❌ Sync Function {func.__name__} failed after {execution_time:.4f} seconds with error: {str(e)}")
            raise

    # Check if the function is async
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper
