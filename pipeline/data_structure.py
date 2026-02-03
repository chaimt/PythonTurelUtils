import json
import logging
from enum import Enum
from typing import Any, Dict, List, Optional

from gcp_files import download_string, upload_string
from global_config import GlobalConfig
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class SessionInfo:
    """
    Lightweight class to store and transport session-related information:
    """

    def __init__(self, zoom_meeting_id: Optional[str] = None):
        self.zoom_meeting_id = zoom_meeting_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "zoom_meeting_id": self.zoom_meeting_id,
        }


class PersonInfo:
    """
    Lightweight class to store and transport expert-related information:
    """

    def __init__(self, id: Optional[int] = None, email: Optional[str] = None):
        self.id = id
        self.email = email

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "email": self.email,
        }


class ExpertInfo(PersonInfo):
    pass


class MembersInfo:
    """
    Lightweight class to store and transport expert-related information:
    """

    def __init__(
        self,
        members: Optional[List[PersonInfo]] = None,
    ):
        self.members = members

    def to_dict(self) -> Dict[str, Any]:
        return {
            "members": [member.to_dict() for member in self.members] if self.members else [],
        }


class ExpertInformation(BaseModel):
    id: Optional[int] = None
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class MemberInformation(BaseModel):
    id: Optional[int] = None
    age: Optional[int] = None
    gender: Optional[str] = None
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class WebhookType(str, Enum):
    FATHOM = "fathom"
    ZOOM = "zoom"


class FileInformation(BaseModel):
    file_name: Optional[str] = None
    file_type: Optional[str] = None
    file_url: Optional[str] = None


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
                    "expert": {"id": 789, "email": "expert@example.com", "first_name": "John"},
                    "session_date": "2024-01-01",
                    "member_information": [{"id": 101, "age": 30, "gender": "M", "email": "member@example.com", "first_name": "Jane"}],
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

    action: Optional[str] = None
    error: Optional[str] = None
    session_id: Optional[str] = None
    account_id: Optional[int] = None
    session_type: Optional[str] = None
    insight_sent: Optional[bool] = None
    expert: ExpertInformation = Field(default_factory=ExpertInformation)
    session_date: Optional[str] = None
    zoom_meeting_id: Optional[str] = None
    member_information: List[MemberInformation] = Field(default_factory=list)
    gcp_path: Optional[str] = None
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

    def get_last_recording_span(self):
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
