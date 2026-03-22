"""Tests for the log_helper module."""

import pytest

# Import the classes to test
from pipeline.log_helper import OurRitualContext


class TestOurRitualContext:
    """Tests for OurRitualContext singleton class."""

    @pytest.fixture(autouse=True)
    def reset_context(self):
        """Reset the context variables before each test."""
        context = OurRitualContext()
        # Reset all context variables to their default values
        context._topic_info_var.set(None)
        context._custom_info_var.set(None)
        yield
        # Clean up after test
        context._topic_info_var.set(None)
        context._custom_info_var.set(None)

    def test_singleton_pattern(self):
        """Test that OurRitualContext follows the singleton pattern."""
        context1 = OurRitualContext()
        context2 = OurRitualContext()
        assert context1 is context2

    def test_set_topic_info(self):
        """Test setting topic info."""
        context = OurRitualContext()
        topic = "test-topic"
        result = context.set_topic_info(topic)

        # Check method chaining
        assert result is context
        # Check value is set
        assert context.topic_info == topic

    def test_topic_info_property(self):
        """Test topic_info property getter."""
        context = OurRitualContext()
        assert context.topic_info is None

        topic = "test-topic"
        context.set_topic_info(topic)
        assert context.topic_info == topic

    def test_custom_info_property(self):
        """Test custom_info property getter."""
        context = OurRitualContext()
        assert context.custom_info is None

        custom = {"key": "value"}
        context._custom_info_var.set(custom)
        assert context.custom_info == custom

    def test_get_extra_empty(self):
        """Test _get_extra returns empty dict when nothing is set."""
        context = OurRitualContext()
        result = context._get_extra()
        assert result == {}

    def test_get_extra_with_custom(self):
        """Test _get_extra includes custom info when set."""
        context = OurRitualContext()
        custom = {"key": "value", "number": 42}
        context._custom_info_var.set(custom)

        result = context._get_extra()
        assert "custom" in result
        assert result["custom"] == custom

    def test_get_extra_merges_with_existing(self):
        """Test _get_extra merges with existing extra dict."""
        context = OurRitualContext()
        custom = {"key": "value"}
        context._custom_info_var.set(custom)

        existing_extra = {"other_field": "other_value"}
        result = context._get_extra(existing_extra)

        assert "custom" in result
        assert result["custom"] == custom
        assert result["other_field"] == "other_value"

    def test_get_extra_existing_overwrites_context(self):
        """Test _get_extra allows existing extra to overwrite context values."""
        context = OurRitualContext()
        custom = {"key": "value"}
        context._custom_info_var.set(custom)

        # Existing extra with same key should overwrite
        custom_override = {"different": "data"}
        existing_extra = {"custom": custom_override}
        result = context._get_extra(existing_extra)

        assert result["custom"] == custom_override

    def test_set_info_to_none(self):
        """Test setting info to None clears the context."""
        context = OurRitualContext()

        # Set values
        topic = "test-topic"
        context.set_topic_info(topic)
        assert context.topic_info is not None

        # Clear by setting to None
        context.set_topic_info(None)
        assert context.topic_info is None

    def test_to_header_empty(self):
        """Test to_header returns empty string when nothing is set."""
        context = OurRitualContext()
        result = context.to_header()
        assert result == ""

    def test_to_header_with_topic(self):
        """Test to_header returns base64url-encoded string with topic."""
        context = OurRitualContext()
        context.set_topic_info("test-topic")

        result = context.to_header()
        # Should be a valid base64url string
        assert result != ""
        assert isinstance(result, str)
        # Base64url should only contain alphanumeric, -, _, and =
        import re

        assert re.match(r"^[A-Za-z0-9_-]+=*$", result)

    def test_to_header_with_custom(self):
        """Test to_header returns base64url-encoded string with custom data."""
        context = OurRitualContext()
        custom = {"user_id": "123", "session_id": "abc"}
        context._custom_info_var.set(custom)

        result = context.to_header()
        # Should be a valid base64url string
        assert result != ""
        assert isinstance(result, str)

    def test_from_header_empty_string(self):
        """Test from_header with empty string returns context with no data set."""
        context = OurRitualContext()
        result = context.from_header("")

        # Check method chaining
        assert result is context
        # Check nothing is set
        assert context.topic_info is None
        assert context.custom_info is None

    def test_from_header_with_topic_only(self):
        """Test from_header parses topic info only."""
        context = OurRitualContext()
        import base64

        header_str = 'ourritual/topic/name: "test-topic"'
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")
        context.from_header(header)

        assert context.topic_info == "test-topic"
        assert context.custom_info is None

    def test_from_header_with_custom_only(self):
        """Test from_header parses custom info."""
        context = OurRitualContext()
        import base64

        header_str = 'ourritual/user_id: "123"\nourritual/session_id: "abc"'
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")
        context.from_header(header)

        assert context.custom_info is not None
        assert context.custom_info["user_id"] == "123"
        assert context.custom_info["session_id"] == "abc"
        assert context.topic_info is None

    def test_from_header_with_all_info(self):
        """Test from_header parses complete context with all sections."""
        context = OurRitualContext()
        import base64

        header_str = 'ourritual/topic/name: "test-topic"\nourritual/user_id: "123"\nourritual/session_id: "abc"'
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")
        context.from_header(header)

        # Check topic info
        assert context.topic_info == "test-topic"

        # Check custom info
        assert context.custom_info is not None
        assert context.custom_info["user_id"] == "123"
        assert context.custom_info["session_id"] == "abc"

    def test_from_header_round_trip(self):
        """Test to_header then from_header returns equivalent data."""
        # Create a context with all data
        context1 = OurRitualContext()
        topic = "test-topic"
        custom = {"user_id": "123", "session_id": "abc"}

        context1.set_topic_info(topic)
        context1._custom_info_var.set(custom)

        # Convert to header
        header = context1.to_header()

        # Create new context and parse from header
        context2 = OurRitualContext()
        # Reset context2 first
        context2._topic_info_var.set(None)
        context2._custom_info_var.set(None)

        context2.from_header(header)

        # Verify data matches
        assert context2.topic_info == topic
        assert context2.custom_info is not None
        assert context2.custom_info["user_id"] == custom["user_id"]
        assert context2.custom_info["session_id"] == custom["session_id"]

    def test_from_header_malformed_lines(self):
        """Test from_header skips invalid lines and parses valid ones."""
        context = OurRitualContext()
        import base64

        header_str = (
            'ourritual/user_id: "123"\n'
            "this is not a valid line\n"
            'ourritual/session_id: "abc"\n'
            "another invalid line without proper format\n"
            'ourritual/topic/name: "test-topic"'
        )
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")
        context.from_header(header)

        # Valid lines should be parsed
        assert context.custom_info is not None
        assert context.custom_info["user_id"] == "123"
        assert context.custom_info["session_id"] == "abc"
        assert context.topic_info == "test-topic"

    def test_from_header_method_chaining(self):
        """Test from_header returns self for chaining."""
        context = OurRitualContext()
        import base64

        header_str = 'ourritual/user_id: "123"'
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")

        result = context.from_header(header)

        assert result is context
        assert context.custom_info is not None

    def test_from_header_overwrites_existing(self):
        """Test calling from_header replaces existing context."""
        context = OurRitualContext()
        import base64

        # Set initial data
        initial_custom = {"user_id": "old"}
        context._custom_info_var.set(initial_custom)

        # Parse new data from header
        header_str = 'ourritual/user_id: "new"'
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")
        context.from_header(header)

        # New data should replace old
        assert context.custom_info["user_id"] == "new"

    def test_from_header_with_whitespace(self):
        """Test from_header handles extra whitespace gracefully."""
        context = OurRitualContext()
        import base64

        header_str = '  ourritual/user_id: "123"  \n\n  ourritual/session_id: "abc"  \n'
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")
        context.from_header(header)

        assert context.custom_info is not None
        assert context.custom_info["user_id"] == "123"
        assert context.custom_info["session_id"] == "abc"

    def test_from_header_invalid_base64(self):
        """Test from_header with invalid base64 returns without error."""
        context = OurRitualContext()
        header = "not-valid-base64!!!"

        result = context.from_header(header)

        # Should return self without crashing
        assert result is context
        # Nothing should be set
        assert context.topic_info is None
        assert context.custom_info is None

    def test_to_list_empty(self):
        """Test to_list returns empty dict when nothing is set."""
        context = OurRitualContext()
        result = context.to_list()
        assert result == {}

    def test_to_list_with_topic(self):
        """Test to_list includes topic info when set."""
        context = OurRitualContext()
        context.set_topic_info("test-topic")

        result = context.to_list()
        assert result == {
            "ourritual/topic/name": "test-topic",
        }

    def test_to_list_with_custom(self):
        """Test to_list includes custom info when set."""
        context = OurRitualContext()
        custom = {"user_id": "123", "session_id": "abc"}
        context._custom_info_var.set(custom)

        result = context.to_list()
        assert "ourritual/user_id" in result
        assert result["ourritual/user_id"] == "123"
        assert "ourritual/session_id" in result
        assert result["ourritual/session_id"] == "abc"

    def test_to_list_with_all_info(self):
        """Test to_list includes all info when all are set."""
        context = OurRitualContext()
        context.set_topic_info("test-topic")
        custom = {"user_id": "123", "session_id": "abc"}
        context._custom_info_var.set(custom)

        result = context.to_list()
        expected = {
            "ourritual/topic/name": "test-topic",
            "ourritual/user_id": "123",
            "ourritual/session_id": "abc",
        }
        assert result == expected

    def test_to_list_filters_none_values(self):
        """Test to_list filters out None values in custom info."""
        context = OurRitualContext()
        custom = {"user_id": "123", "session_id": None}
        context._custom_info_var.set(custom)

        result = context.to_list()
        # Only user_id should be included, session_id should be filtered out
        assert result == {"ourritual/user_id": "123"}

    def test_to_list_converts_values_to_strings(self):
        """Test to_list converts all values to strings."""
        context = OurRitualContext()
        custom = {"user_id": 123, "amount": 45.67, "active": True}
        context._custom_info_var.set(custom)

        result = context.to_list()
        assert result["ourritual/user_id"] == "123"
        assert result["ourritual/amount"] == "45.67"
        assert result["ourritual/active"] == "True"
