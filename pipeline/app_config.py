import json
import logging
import os
from abc import ABC
from enum import Enum
from threading import Lock
from typing import Any, Dict, Type

from google.cloud import secretmanager
from pydantic import ConfigDict
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource


class LogType(str, Enum):
    LOCAL = "local"
    GCP = "gcp"
    K8s = "k8s"


logger = logging.getLogger(__name__)


# Global variable to track the last metric timestamp to ensure chronological order
_metric_lock = Lock()
_last_metric_timestamp = 0


def access_secret_version(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """
    Access the secret version and return the payload.

    Args:
        project_id: GCP project ID
        secret_id: Name of the secret in Secret Manager
        version_id: Version of the secret (default is "latest")

    Returns:
        Secret payload as a string
    """
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = client.access_secret_version(request={"name": name})

    # Return the decoded payload
    return response.payload.data.decode("UTF-8")


def _flatten_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    flattened_dict = {}
    for key, value in d.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flattened_dict[sub_key] = sub_value
        else:
            flattened_dict[key] = value
    return flattened_dict


def load_secrets(gcp_project_id: str, service_name: str):
    try:
        common_config = access_secret_version(gcp_project_id, "common")
        common_dict = json.loads(common_config)
        common_dict = _flatten_dict(common_dict)
    except Exception as e:
        logger.warning(f"⚠️ Error loading common config: {e}")
        common_dict = {}

    try:
        app_config = access_secret_version(gcp_project_id, service_name)
        app_dict = json.loads(app_config)
        app_dict = _flatten_dict(app_dict)
    except Exception as e:
        logger.warning(f"⚠️ Error loading common config: {e}")
        app_dict = {}

    return common_dict | app_dict


class DictionarySettingsSource(PydanticBaseSettingsSource):
    """
    A custom settings source that reads settings from a dictionary.
    """

    def __init__(self, settings_cls: Type[BaseSettings], dictionary: Dict[str, Any]):
        super().__init__(settings_cls)
        self._dictionary = dictionary

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        # def get_field_value(
        #     self, field: Field, field_name: str
        # ) -> Tuple[Any, str, bool]:
        """
        Get the value for a field from the dictionary.
        """
        # You can add custom logic here if needed, e.g., handling nested keys
        field_value = self._dictionary.get(field_name)
        return field_value, field_name, False  # Return value, key_name, value_is_complex

    def __call__(self) -> Dict[str, Any]:
        """
        Return the dictionary containing the settings.
        """
        return self._dictionary


class AppSettings(ABC, BaseSettings):
    model_config = ConfigDict(
        env_file=".env",  # Optional: load from .env file if present
        env_file_encoding="utf-8",
        extra="allow",
        case_sensitive=False,
    )

    git_commit: str = ""
    release_date: str = ""
    version: str = "0.0.1"
    sentry_live: bool = False
    sentry_url: str = ""
    env: str = "dev"
    gcp_project_id: str = ""
    service_name: str = "unknown"
    log_type: LogType = LogType.LOCAL
    log_level: str = "INFO"
    management_url: str = "http://127.0.0.1:8000"
    max_messages: int = 1
    authentication_url: str = ""
    bucket_name: str = ""
    incoming_topic: str = ""
    outgoing_topic: str = ""
    management_topic: str = "management"
    dlq_subscription: str = ""
    running_locally: bool = False
    enable_system_metrics: bool = False
    enable_app_metrics: bool = False
    enable_app_instrumentation: bool = False
    cache_ttl_seconds: int = 1 * 60 * 60

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        COMMON_SECRETS = os.getenv("COMMON_SECRETS", "")
        common_secrets = _flatten_dict(json.loads(COMMON_SECRETS)) if COMMON_SECRETS else {}
        SERVICE_SECRETS = os.getenv("SERVICE_SECRETS", "")
        service_secrets = _flatten_dict(json.loads(SERVICE_SECRETS)) if SERVICE_SECRETS else {}
        dict_settings = common_secrets | service_secrets
        for key, value in dict_settings.items():
            os.environ[key] = str(value)
        return (init_settings, env_settings, dotenv_settings, file_secret_settings, DictionarySettingsSource(settings_cls, dict_settings))


class AppGlobalConfigBase:
    def to_dict(self) -> dict:
        return {}

    def clear_all_prompts(self):
        pass
