"""Tests for the llm_utils module."""

import re


# NOTE: This is an inline copy of clean_json_response from pipeline.llm_utils
# to avoid import dependencies on heavy modules (langfuse, opentelemetry, etc.)
# If the implementation in llm_utils.py changes, update this copy as well.
def clean_json_response(response: str) -> str:
    """
    Clean LLM response by removing markdown code block formatting.

    Handles responses like:
    ```json
    {"key": "value"}
    ```

    Or:
    ```
    {"key": "value"}
    ```

    Args:
        response: The LLM response string that may contain markdown code blocks

    Returns:
        Cleaned JSON string without markdown formatting
    """
    if not response:
        return response

    # Remove ```json or ``` markers at the start and end
    # Pattern matches optional whitespace, code fence with optional 'json' (case-insensitive),
    # captures content, then matches closing code fence
    pattern = r"^\s*```(?:json)?\s*\n?(.*?)\n?```\s*$"
    match = re.match(pattern, response.strip(), re.DOTALL | re.IGNORECASE)

    if match:
        return match.group(1).strip()

    # If no markdown blocks found, return original response stripped
    return response.strip()


class TestCleanJsonResponse:
    """Tests for clean_json_response function."""

    def test_json_with_json_marker(self):
        """Test cleaning response with ```json marker."""
        response = """```json
{"name": "test", "value": 123}
```"""
        result = clean_json_response(response)
        assert result == '{"name": "test", "value": 123}'

    def test_json_with_plain_marker(self):
        """Test cleaning response with plain ``` marker."""
        response = """```
{"name": "test", "value": 123}
```"""
        result = clean_json_response(response)
        assert result == '{"name": "test", "value": 123}'

    def test_json_without_markers(self):
        """Test response without markdown markers returns as-is."""
        response = '{"name": "test", "value": 123}'
        result = clean_json_response(response)
        assert result == '{"name": "test", "value": 123}'

    def test_multiline_json(self):
        """Test cleaning multiline JSON with formatting."""
        response = """```json
{
  "name": "test",
  "value": 123,
  "nested": {
    "key": "value"
  }
}
```"""
        expected = """{
  "name": "test",
  "value": 123,
  "nested": {
    "key": "value"
  }
}"""
        result = clean_json_response(response)
        assert result == expected

    def test_json_with_extra_whitespace(self):
        """Test cleaning handles extra whitespace around markers."""
        response = """   ```json
{"name": "test"}
```   """
        result = clean_json_response(response)
        assert result == '{"name": "test"}'

    def test_json_with_leading_trailing_newlines(self):
        """Test cleaning handles newlines around JSON content."""
        response = """```json

{"name": "test"}

```"""
        result = clean_json_response(response)
        assert result == '{"name": "test"}'

    def test_empty_string(self):
        """Test empty string returns empty string."""
        result = clean_json_response("")
        assert result == ""

    def test_none_value(self):
        """Test None value returns None."""
        result = clean_json_response(None)
        assert result is None

    def test_whitespace_only(self):
        """Test whitespace-only string returns empty."""
        result = clean_json_response("   \n\t   ")
        assert result == ""

    def test_json_array(self):
        """Test cleaning JSON array."""
        response = """```json
[{"id": 1}, {"id": 2}, {"id": 3}]
```"""
        result = clean_json_response(response)
        assert result == '[{"id": 1}, {"id": 2}, {"id": 3}]'

    def test_json_with_string_values_containing_braces(self):
        """Test JSON with string values that contain braces."""
        response = """```json
{"template": "Hello {name}, welcome!", "value": 123}
```"""
        result = clean_json_response(response)
        assert result == '{"template": "Hello {name}, welcome!", "value": 123}'

    def test_json_with_escaped_characters(self):
        """Test JSON with escaped characters."""
        response = """```json
{"path": "C:\\\\Users\\\\test", "message": "Line 1\\nLine 2"}
```"""
        result = clean_json_response(response)
        assert result == '{"path": "C:\\\\Users\\\\test", "message": "Line 1\\nLine 2"}'

    def test_incomplete_markdown_opening_only(self):
        """Test response with opening marker but no closing returns as-is."""
        response = """```json
{"name": "test"}"""
        result = clean_json_response(response)
        # Should return stripped version since pattern doesn't match
        assert result == '```json\n{"name": "test"}'

    def test_incomplete_markdown_closing_only(self):
        """Test response with closing marker but no opening returns as-is."""
        response = """{"name": "test"}
```"""
        result = clean_json_response(response)
        # Should return stripped version since pattern doesn't match
        assert result == '{"name": "test"}\n```'

    def test_json_with_no_newline_after_opening_marker(self):
        """Test JSON immediately after opening marker."""
        response = '```json{"name": "test"}```'
        result = clean_json_response(response)
        assert result == '{"name": "test"}'

    def test_json_with_language_identifier_uppercase(self):
        """Test cleaning works with uppercase JSON marker (case-insensitive)."""
        response = """```JSON
{"name": "test"}
```"""
        result = clean_json_response(response)
        # Pattern matches case-insensitively
        assert result == '{"name": "test"}'

    def test_complex_nested_json(self):
        """Test cleaning complex nested JSON structure."""
        response = """```json
{
  "user": {
    "id": 123,
    "profile": {
      "name": "John Doe",
      "tags": ["admin", "user"]
    }
  },
  "metadata": {
    "created": "2024-01-01",
    "updated": "2024-01-26"
  }
}
```"""
        expected = """{
  "user": {
    "id": 123,
    "profile": {
      "name": "John Doe",
      "tags": ["admin", "user"]
    }
  },
  "metadata": {
    "created": "2024-01-01",
    "updated": "2024-01-26"
  }
}"""
        result = clean_json_response(response)
        assert result == expected

    def test_json_with_unicode_characters(self):
        """Test JSON with unicode characters."""
        response = """```json
{"message": "Hello 世界", "emoji": "🎉"}
```"""
        result = clean_json_response(response)
        assert result == '{"message": "Hello 世界", "emoji": "🎉"}'

    def test_multiple_code_blocks_only_outer_removed(self):
        """Test that only outer code block markers are removed."""
        response = """```json
{"code": "```python\\nprint('hello')\\n```"}
```"""
        result = clean_json_response(response)
        assert result == '{"code": "```python\\nprint(\'hello\')\\n```"}'


class TestCleanJsonResponseValidation:
    """Tests that validate cleaned responses are valid parseable JSON."""

    def test_simple_object_is_valid_json(self):
        """Test that cleaned simple JSON object can be parsed."""
        import json

        response = """```json
{"name": "test", "value": 123, "active": true}
```"""
        result = clean_json_response(response)
        parsed = json.loads(result)

        assert parsed["name"] == "test"
        assert parsed["value"] == 123
        assert parsed["active"] is True

    def test_array_is_valid_json(self):
        """Test that cleaned JSON array can be parsed."""
        import json

        response = """```json
[1, 2, 3, 4, 5]
```"""
        result = clean_json_response(response)
        parsed = json.loads(result)

        assert isinstance(parsed, list)
        assert parsed == [1, 2, 3, 4, 5]

    def test_nested_object_is_valid_json(self):
        """Test that cleaned nested JSON can be parsed."""
        import json

        response = """```json
{
  "user": {
    "id": 123,
    "name": "John Doe",
    "profile": {
      "email": "john@example.com",
      "tags": ["admin", "user"]
    }
  }
}
```"""
        result = clean_json_response(response)
        parsed = json.loads(result)

        assert parsed["user"]["id"] == 123
        assert parsed["user"]["name"] == "John Doe"
        assert parsed["user"]["profile"]["email"] == "john@example.com"
        assert parsed["user"]["profile"]["tags"] == ["admin", "user"]

    def test_array_of_objects_is_valid_json(self):
        """Test that cleaned array of objects can be parsed."""
        import json

        response = """```json
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"},
  {"id": 3, "name": "Charlie"}
]
```"""
        result = clean_json_response(response)
        parsed = json.loads(result)

        assert isinstance(parsed, list)
        assert len(parsed) == 3
        assert parsed[0]["name"] == "Alice"
        assert parsed[1]["name"] == "Bob"
        assert parsed[2]["name"] == "Charlie"

    def test_json_with_null_values_is_valid(self):
        """Test that cleaned JSON with null values can be parsed."""
        import json

        response = """```json
{"name": "test", "value": null, "optional": null}
```"""
        result = clean_json_response(response)
        parsed = json.loads(result)

        assert parsed["name"] == "test"
        assert parsed["value"] is None
        assert parsed["optional"] is None

    def test_json_with_escaped_characters_is_valid(self):
        """Test that cleaned JSON with escaped characters can be parsed."""
        import json

        response = """```json
{"path": "C:\\\\Users\\\\test", "message": "Line 1\\nLine 2\\tTabbed"}
```"""
        result = clean_json_response(response)
        parsed = json.loads(result)

        assert parsed["path"] == "C:\\Users\\test"
        assert parsed["message"] == "Line 1\nLine 2\tTabbed"

    def test_json_with_unicode_is_valid(self):
        """Test that cleaned JSON with unicode characters can be parsed."""
        import json

        response = """```json
{"message": "Hello 世界", "emoji": "🎉", "symbol": "€"}
```"""
        result = clean_json_response(response)
        parsed = json.loads(result)

        assert parsed["message"] == "Hello 世界"
        assert parsed["emoji"] == "🎉"
        assert parsed["symbol"] == "€"

    def test_json_with_numbers_is_valid(self):
        """Test that cleaned JSON with various number types can be parsed."""
        import json

        response = """```json
{
  "integer": 42,
  "negative": -10,
  "float": 3.14,
  "scientific": 1.23e-4,
  "zero": 0
}
```"""
        result = clean_json_response(response)
        parsed = json.loads(result)

        assert parsed["integer"] == 42
        assert parsed["negative"] == -10
        assert parsed["float"] == 3.14
        assert parsed["scientific"] == 1.23e-4
        assert parsed["zero"] == 0

    def test_json_with_empty_structures_is_valid(self):
        """Test that cleaned JSON with empty objects/arrays can be parsed."""
        import json

        response = """```json
{
  "empty_object": {},
  "empty_array": [],
  "nested_empty": {"inner": {}}
}
```"""
        result = clean_json_response(response)
        parsed = json.loads(result)

        assert parsed["empty_object"] == {}
        assert parsed["empty_array"] == []
        assert parsed["nested_empty"]["inner"] == {}

    def test_json_with_boolean_values_is_valid(self):
        """Test that cleaned JSON with boolean values can be parsed."""
        import json

        response = """```json
{"is_active": true, "is_deleted": false, "has_permission": true}
```"""
        result = clean_json_response(response)
        parsed = json.loads(result)

        assert parsed["is_active"] is True
        assert parsed["is_deleted"] is False
        assert parsed["has_permission"] is True

    def test_plain_json_without_markers_is_valid(self):
        """Test that plain JSON without markdown markers can still be parsed."""
        import json

        response = '{"name": "test", "value": 123}'
        result = clean_json_response(response)
        parsed = json.loads(result)

        assert parsed["name"] == "test"
        assert parsed["value"] == 123

    def test_uppercase_json_marker_produces_valid_json(self):
        """Test that uppercase JSON marker produces valid parseable JSON."""
        import json

        response = """```JSON
{"name": "test", "value": 456}
```"""
        result = clean_json_response(response)
        parsed = json.loads(result)

        assert parsed["name"] == "test"
        assert parsed["value"] == 456
