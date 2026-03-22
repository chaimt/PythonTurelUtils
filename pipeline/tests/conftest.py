"""Pytest configuration for pipeline tests."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add pipeline directory to path so modules can find each other
# This is needed because modules use flat imports (e.g., 'from global_config import singleton')
pipeline_dir = Path(__file__).parent.parent
sys.path.insert(0, str(pipeline_dir))

# Mock modules that aren't installed in the test environment but are imported at module level
# These mocks must be set up BEFORE any module imports them
_mock_modules = [
    "google.cloud.pubsub_v1",
    "google.cloud.storage",
    "google.cloud.storage.bucket",
]

for mod_name in _mock_modules:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()


def pytest_collection_modifyitems(items):
    """
    Modify test collection order to ensure test_log_helper runs before test_llm_utils.

    This is necessary because test_llm_utils mocks certain modules that test_log_helper
    depends on. By running test_log_helper first, we ensure it gets the real implementations.
    """
    # Sort test items so that test_log_helper tests run before test_llm_utils tests
    items.sort(
        key=lambda item: ("test_llm_utils" in str(item.fspath), item.nodeid)  # llm_utils tests go last  # maintain original order within each file
    )
