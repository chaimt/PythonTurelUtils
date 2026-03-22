"""Tests for the log_helper module."""

import pytest

# Import the classes to test
# Use flat import to match how pipeline_utils.py imports (ensures same singleton instance)
from log_helper import AppContext


class TestAppContext:
    """Tests for AppContext singleton class."""

    @pytest.fixture(autouse=True)
    def reset_context(self):
        """Reset the context variables before each test."""
        context = AppContext()
        # Reset all context variables to their default values
        context._topic_info_var.set(None)
        context._custom_info_var.set(None)
        yield
        # Clean up after test
        context._topic_info_var.set(None)
        context._custom_info_var.set(None)

    def test_singleton_pattern(self):
        """Test that AppContext follows the singleton pattern."""
        context1 = AppContext()
        context2 = AppContext()
        assert context1 is context2

    def test_set_topic_info(self):
        """Test setting topic info."""
        context = AppContext()
        topic = "test-topic"
        result = context.set_topic_info(topic)

        # Check method chaining
        assert result is context
        # Check value is set
        assert context.topic_info == topic

    def test_topic_info_property(self):
        """Test topic_info property getter."""
        context = AppContext()
        assert context.topic_info is None

        topic = "test-topic"
        context.set_topic_info(topic)
        assert context.topic_info == topic

    def test_custom_info_property(self):
        """Test custom_info property getter."""
        context = AppContext()
        assert context.custom_info is None

        custom = {"key": "value"}
        context._custom_info_var.set(custom)
        assert context.custom_info == custom

    def test_get_extra_empty(self):
        """Test _get_extra returns empty dict when nothing is set."""
        context = AppContext()
        result = context._get_extra()
        assert result == {}

    def test_get_extra_with_custom(self):
        """Test _get_extra includes custom info when set."""
        context = AppContext()
        custom = {"key": "value", "number": 42}
        context._custom_info_var.set(custom)

        result = context._get_extra()
        assert "custom" in result
        assert result["custom"] == custom

    def test_get_extra_merges_with_existing(self):
        """Test _get_extra merges with existing extra dict."""
        context = AppContext()
        custom = {"key": "value"}
        context._custom_info_var.set(custom)

        existing_extra = {"other_field": "other_value"}
        result = context._get_extra(existing_extra)

        assert "custom" in result
        assert result["custom"] == custom
        assert result["other_field"] == "other_value"

    def test_get_extra_existing_overwrites_context(self):
        """Test _get_extra allows existing extra to overwrite context values."""
        context = AppContext()
        custom = {"key": "value"}
        context._custom_info_var.set(custom)

        # Existing extra with same key should overwrite
        custom_override = {"different": "data"}
        existing_extra = {"custom": custom_override}
        result = context._get_extra(existing_extra)

        assert result["custom"] == custom_override

    def test_set_info_to_none(self):
        """Test setting info to None clears the context."""
        context = AppContext()

        # Set values
        topic = "test-topic"
        context.set_topic_info(topic)
        assert context.topic_info is not None

        # Clear by setting to None
        context.set_topic_info(None)
        assert context.topic_info is None

    def test_to_header_empty(self):
        """Test to_header returns empty string when nothing is set."""
        context = AppContext()
        result = context.to_header()
        assert result == ""

    def test_to_header_with_topic(self):
        """Test to_header returns base64url-encoded string with topic."""
        context = AppContext()
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
        context = AppContext()
        custom = {"user_id": "123", "session_id": "abc"}
        context._custom_info_var.set(custom)

        result = context.to_header()
        # Should be a valid base64url string
        assert result != ""
        assert isinstance(result, str)

    def test_from_header_empty_string(self):
        """Test from_header with empty string returns context with no data set."""
        context = AppContext()
        result = context.from_header("")

        # Check method chaining
        assert result is context
        # Check nothing is set
        assert context.topic_info is None
        assert context.custom_info is None

    def test_from_header_with_topic_only(self):
        """Test from_header parses topic info only."""
        context = AppContext()
        import base64

        header_str = 'topic/name: "test-topic"'
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")
        context.from_header(header)

        assert context.topic_info == "test-topic"
        assert context.custom_info is None

    def test_from_header_with_custom_only(self):
        """Test from_header parses custom info."""
        context = AppContext()
        import base64

        header_str = 'user_id: "123"\nsession_id: "abc"'
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")
        context.from_header(header)

        assert context.custom_info is not None
        assert context.custom_info["user_id"] == "123"
        assert context.custom_info["session_id"] == "abc"
        assert context.topic_info is None

    def test_from_header_with_all_info(self):
        """Test from_header parses complete context with all sections."""
        context = AppContext()
        import base64

        header_str = 'topic/name: "test-topic"\nuser_id: "123"\nsession_id: "abc"'
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")
        context.from_header(header)

        # Check topic info
        assert context.topic_info == "test-topic"

        # Check custom info
        assert context.custom_info is not None
        assert context.custom_info["user_id"] == "123"
        assert context.custom_info["session_id"] == "abc"

    def test_from_header_round_trip(self):
        """Test to_header and from_header round-trip topic and custom_info."""
        context1 = AppContext()
        topic = "test-topic"
        custom = {"user_id": "123", "session_id": "abc"}

        context1.set_topic_info(topic)
        context1._custom_info_var.set(custom)

        header = context1.to_header()
        assert header != ""

        context2 = AppContext()
        context2._topic_info_var.set(None)
        context2._custom_info_var.set(None)
        context2.from_header(header)

        assert context2.topic_info == topic
        assert context2.custom_info == custom

    def test_from_header_malformed_lines(self):
        """Test from_header skips invalid lines and parses valid ones."""
        context = AppContext()
        import base64

        header_str = (
            'user_id: "123"\nthis is not a valid line\nsession_id: "abc"\nanother invalid line without proper format\ntopic/name: "test-topic"'
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
        context = AppContext()
        import base64

        header_str = 'user_id: "123"'
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")

        result = context.from_header(header)

        assert result is context
        assert context.custom_info is not None

    def test_from_header_overwrites_existing(self):
        """Test calling from_header replaces existing context."""
        context = AppContext()
        import base64

        # Set initial data
        initial_custom = {"user_id": "old"}
        context._custom_info_var.set(initial_custom)

        # Parse new data from header
        header_str = 'user_id: "new"'
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")
        context.from_header(header)

        # New data should replace old
        assert context.custom_info["user_id"] == "new"

    def test_from_header_with_whitespace(self):
        """Test from_header handles extra whitespace gracefully."""
        context = AppContext()
        import base64

        header_str = '  user_id: "123"  \n\n  session_id: "abc"  \n'
        header = base64.urlsafe_b64encode(header_str.encode("utf-8")).decode("ascii")
        context.from_header(header)

        assert context.custom_info is not None
        assert context.custom_info["user_id"] == "123"
        assert context.custom_info["session_id"] == "abc"

    def test_from_header_invalid_base64(self):
        """Test from_header with invalid base64 returns without error."""
        context = AppContext()
        header = "not-valid-base64!!!"

        result = context.from_header(header)

        # Should return self without crashing
        assert result is context
        # Nothing should be set
        assert context.topic_info is None
        assert context.custom_info is None

    def test_to_list_empty(self):
        """Test to_list returns empty dict when nothing is set."""
        context = AppContext()
        result = context.to_list()
        assert result == {}

    def test_to_list_with_topic(self):
        """Test to_list includes topic info when set."""
        context = AppContext()
        context.set_topic_info("test-topic")

        result = context.to_list()
        assert result == {"topic/name": "test-topic"}

    def test_to_list_with_custom(self):
        """Test to_list includes custom info when set."""
        context = AppContext()
        custom = {"user_id": "123", "session_id": "abc"}
        context._custom_info_var.set(custom)

        result = context.to_list()
        assert "user_id" in result
        assert result["user_id"] == "123"
        assert "session_id" in result
        assert result["session_id"] == "abc"

    def test_to_list_with_all_info(self):
        """Test to_list includes all info when all are set."""
        context = AppContext()
        context.set_topic_info("test-topic")
        custom = {"user_id": "123", "session_id": "abc"}
        context._custom_info_var.set(custom)

        result = context.to_list()
        expected = {
            "topic/name": "test-topic",
            "user_id": "123",
            "session_id": "abc",
        }
        assert result == expected

    def test_to_list_filters_none_values(self):
        """Test to_list filters out None values in custom info."""
        context = AppContext()
        custom = {"user_id": "123", "session_id": None}
        context._custom_info_var.set(custom)

        result = context.to_list()
        # Only user_id should be included, session_id should be filtered out
        assert result == {"user_id": "123"}

    def test_to_list_converts_values_to_strings(self):
        """Test to_list converts all values to strings."""
        context = AppContext()
        custom = {"user_id": 123, "amount": 45.67, "active": True}
        context._custom_info_var.set(custom)

        result = context.to_list()
        assert result["user_id"] == "123"
        assert result["amount"] == "45.67"
        assert result["active"] == "True"


class TestAppContextWithSessionInformation:
    """Tests for AppContext using SessionInformation.add_to_context()."""

    @pytest.fixture(autouse=True)
    def reset_context(self):
        """Reset the context variables before each test."""
        context = AppContext()
        # Reset all context variables to their default values
        context._topic_info_var.set(None)
        context._custom_info_var.set(None)
        yield
        # Clean up after test
        context._topic_info_var.set(None)
        context._custom_info_var.set(None)

    def test_add_to_context_basic(self):
        """Test SessionInformation.add_to_context() populates AppContext."""
        from pipeline_utils import ExpertInformation, SessionInformation

        session = SessionInformation(
            session_id="sess-123",
            expert=ExpertInformation(id=456, email="expert@example.com"),
            zoom_meeting_id="zoom-789",
        )

        session.add_to_context()

        context = AppContext()
        assert context.custom_info is not None
        assert context.custom_info["session/id"] == "sess-123"
        assert context.custom_info["session/expert/id"] == 456
        assert context.custom_info["session/expert/email"] == "expert@example.com"
        assert context.custom_info["session/zoom_meeting_id"] == "zoom-789"

        header = context.to_header()
        assert header != ""
        context._topic_info_var.set(None)
        context._custom_info_var.set(None)
        context.from_header(header)
        assert context.custom_info is not None
        assert context.custom_info["session/id"] == "sess-123"
        assert context.custom_info["session/expert/id"] == "456"
        assert context.custom_info["session/expert/email"] == "expert@example.com"
        assert context.custom_info["session/zoom_meeting_id"] == "zoom-789"

        context._custom_info_var.set(
            {
                "session/id": "sess-123",
                "session/expert/id": 456,
                "session/expert/email": "expert@example.com",
                "session/zoom_meeting_id": "zoom-789",
            }
        )
        list_result = context.to_list()
        assert list_result["session/id"] == "sess-123"
        assert list_result["session/expert/id"] == "456"
        assert list_result["session/expert/email"] == "expert@example.com"
        assert list_result["session/zoom_meeting_id"] == "zoom-789"

    def test_add_to_context_with_members(self):
        """Test SessionInformation.add_to_context() includes member information."""
        from pipeline_utils import (
            ExpertInformation,
            MemberInformation,
            SessionInformation,
        )

        session = SessionInformation(
            session_id="sess-123",
            expert=ExpertInformation(id=456, email="expert@example.com"),
            member_information=[
                MemberInformation(id=101, email="member1@example.com"),
                MemberInformation(id=102, email="member2@example.com"),
            ],
        )

        session.add_to_context()

        context = AppContext()
        assert context.custom_info is not None
        assert context.custom_info["members/0/id"] == 101
        assert context.custom_info["members/0/email"] == "member1@example.com"
        assert context.custom_info["members/1/id"] == 102
        assert context.custom_info["members/1/email"] == "member2@example.com"

    def test_add_to_context_to_header_round_trip(self):
        """Test to_header / from_header round-trip for session context."""
        from pipeline_utils import ExpertInformation, SessionInformation

        session = SessionInformation(
            session_id="sess-abc",
            expert=ExpertInformation(id=789, email="test@example.com"),
            zoom_meeting_id="zoom-xyz",
        )

        session.add_to_context()

        context = AppContext()
        header = context.to_header()

        assert header != ""

        context._topic_info_var.set(None)
        context._custom_info_var.set(None)
        context.from_header(header)

        assert context.custom_info is not None
        assert context.custom_info["session/id"] == "sess-abc"
        assert context.custom_info["session/expert/id"] == "789"
        assert context.custom_info["session/expert/email"] == "test@example.com"
        assert context.custom_info["session/zoom_meeting_id"] == "zoom-xyz"

    def test_add_to_context_with_members_to_header_round_trip(self):
        """Test to_header / from_header round-trip including member fields (values are strings)."""
        from pipeline_utils import (
            ExpertInformation,
            MemberInformation,
            SessionInformation,
        )

        session = SessionInformation(
            session_id="sess-full",
            expert=ExpertInformation(id=100, email="expert@test.com"),
            zoom_meeting_id="zoom-full",
            member_information=[
                MemberInformation(id=201, email="m1@test.com"),
                MemberInformation(id=202, email="m2@test.com"),
            ],
        )

        session.add_to_context()

        context = AppContext()
        header = context.to_header()
        assert header != ""

        context._topic_info_var.set(None)
        context._custom_info_var.set(None)
        context.from_header(header)

        assert context.custom_info is not None
        assert context.custom_info["session/id"] == "sess-full"
        assert context.custom_info["members/0/id"] == "201"
        assert context.custom_info["members/0/email"] == "m1@test.com"
        assert context.custom_info["members/1/id"] == "202"
        assert context.custom_info["members/1/email"] == "m2@test.com"

    def test_add_to_context_combined_with_topic(self):
        """Test to_header / from_header round-trip with session and topic."""
        from pipeline_utils import ExpertInformation, SessionInformation

        session = SessionInformation(
            session_id="sess-topic",
            expert=ExpertInformation(id=555, email="topic@example.com"),
        )

        session.add_to_context()

        context = AppContext()
        context.set_topic_info("test-topic-name")

        header = context.to_header()
        assert header != ""

        context._topic_info_var.set(None)
        context._custom_info_var.set(None)
        context.from_header(header)

        assert context.topic_info == "test-topic-name"
        assert context.custom_info is not None
        assert context.custom_info["session/id"] == "sess-topic"
        assert context.custom_info["session/expert/id"] == "555"
        assert context.custom_info["session/expert/email"] == "topic@example.com"

    def test_add_to_context_to_list_format(self):
        """Test SessionInformation data appears correctly in to_list() output."""
        from pipeline_utils import ExpertInformation, SessionInformation

        session = SessionInformation(
            session_id="sess-list",
            expert=ExpertInformation(id=333, email="list@example.com"),
            zoom_meeting_id="zoom-list",
        )

        session.add_to_context()

        context = AppContext()
        result = context.to_list()

        assert "session/id" in result
        assert result["session/id"] == "sess-list"
        assert "session/expert/id" in result
        assert result["session/expert/id"] == "333"
        assert "session/expert/email" in result
        assert result["session/expert/email"] == "list@example.com"
        assert "session/zoom_meeting_id" in result
        assert result["session/zoom_meeting_id"] == "zoom-list"

    def test_header_format_verification(self):
        """Test the header contains expected base64url format."""
        import base64
        import re

        from pipeline_utils import ExpertInformation, SessionInformation

        session = SessionInformation(
            session_id="sess-format",
            expert=ExpertInformation(id=111, email="format@test.com"),
        )

        session.add_to_context()

        context = AppContext()
        header = context.to_header()

        # Should be valid base64url
        assert re.match(r"^[A-Za-z0-9_-]+=*$", header)

        # Decode and verify structure matches to_list / to_header
        decoded = base64.urlsafe_b64decode(header.encode("ascii")).decode("utf-8")
        assert 'session/id: "sess-format"' in decoded
        assert 'session/expert/id: "111"' in decoded
        assert 'session/expert/email: "format@test.com"' in decoded
