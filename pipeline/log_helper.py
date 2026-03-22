import contextvars
from typing import Any, Dict, Optional

from global_config import singleton

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


@singleton
class AppContext:
    """
    A wrapper class for Python's logging.Logger that automatically appends
    member_id, expert_id, and session_zoom_info as extra data to all log messages.

    """

    # Context variables for storing context data
    _topic_info_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("topic", default=None)
    _custom_info_var: contextvars.ContextVar[Optional[Dict[str, Any]]] = contextvars.ContextVar("custom_info", default=None)

    def set_topic_info(self, topic: Optional[str]) -> Self:
        """Set the topic information to include in all log messages.

        Returns:
            Self for method chaining
        """
        self._topic_info_var.set(topic)
        return self

    def set_custom_info(self, key: str, value: Any) -> Self:
        """Set a custom key-value pair in the context.

        Args:
            key: The key for the custom info (e.g., 'session/id', 'session/expert/id')
            value: The value to store

        Returns:
            Self for method chaining
        """
        current = self._custom_info_var.get()
        if current is None:
            current = {}
        current[key] = value
        self._custom_info_var.set(current)
        return self

    def _get_extra(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Build the extra dictionary with context fields.

        Args:
            extra: Optional existing extra dictionary to merge with

        Returns:
            Dictionary with context fields merged with existing extra data
        """
        context_extra: Dict[str, Any] = {}
        custom_info = self._custom_info_var.get()
        if custom_info is not None:
            context_extra["custom"] = custom_info

        if extra:
            context_extra.update(extra)

        return context_extra

    @property
    def topic_info(self) -> Optional[str]:
        """Get the current topic_info."""
        return self._topic_info_var.get()

    @property
    def custom_info(self) -> Optional[Dict[str, Any]]:
        """Get the current generic_info."""
        return self._custom_info_var.get()

    def to_list(self) -> Dict[str, str]:
        """Convert the context to a dictionary of key-value pairs.

        Returns:
            Dictionary with keys in format 'app_context/section/key' and string values.
            Returns empty dict if no context is set.
        """
        result = {}
        if self.topic_info is not None:
            result["topic/name"] = self.topic_info

        if self.custom_info is not None:
            for key, value in self.custom_info.items():
                if value is not None:
                    result[f"{key}"] = str(value)

        return result

    def to_header(self) -> str:
        """Convert the context to a header string using base64url encoding.

        Returns:
            Base64url-encoded string containing the context data.
            Returns empty string if no context is set.
        """
        import base64

        json_dict = self.to_list()
        if not json_dict:
            return ""

        # Convert dict to list of "key: value" strings, join with newlines and encode to base64url
        json_list = [f'{key}: "{value}"' for key, value in json_dict.items()]
        json_str = "\n".join(json_list)
        encoded = base64.urlsafe_b64encode(json_str.encode("utf-8")).decode("ascii")
        return encoded

    def from_dict(self, data: Dict[str, Any]) -> Self:
        """Add key-value pairs from a dictionary to the context.

        Args:
            data: Dictionary of key-value pairs. Key "topic/name" sets topic_info;
                  all other keys are added to custom_info.

        Returns:
            Self for method chaining
        """
        if not data:
            return self
        for key, value in data.items():
            if key == "topic/name":
                self.set_topic_info(str(value) if value is not None else None)
            else:
                self.set_custom_info(key, value)
        return self

    def from_header(self, header: str) -> Self:
        """Reconstruct the context from a base64url-encoded header string produced by to_header().

        Args:
            header: Base64url-encoded string produced by to_header()

        Returns:
            Self for method chaining
        """
        import base64
        import re

        if not header or not header.strip():
            return self

        try:
            # Decode from base64url
            decoded_bytes = base64.urlsafe_b64decode(header.encode("ascii"))
            decoded_str = decoded_bytes.decode("utf-8")
        except Exception:
            # If decoding fails, return self unchanged
            return self

        # Header is the full snapshot: clear existing context before applying
        self._topic_info_var.set(None)
        self._custom_info_var.set(None)

        # Same line format as to_header() / to_list(): key: "value"
        line_pattern = re.compile(r'^(.+):\s*"([^"]*)"\s*$')

        topic_data: Optional[str] = None
        custom_data: Dict[str, Any] = {}

        for line in decoded_str.split("\n"):
            line = line.strip()
            if not line:
                continue

            match = line_pattern.match(line)
            if not match:
                continue

            key, value = match.group(1), match.group(2)
            if key == "topic/name":
                topic_data = value
            else:
                custom_data[key] = value

        if topic_data is not None:
            self.set_topic_info(topic_data)

        if custom_data:
            self._custom_info_var.set(custom_data)

        return self
