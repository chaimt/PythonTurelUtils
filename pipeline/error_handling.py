import logging

from global_config import GlobalConfig
from pipeline_utils import get_authentication_token

logger = logging.getLogger(__name__)


def handle_http_authentication_error(http_err, response, authentication_url, method_name, url=None, data=None):
    if "401 Unauthorized" in str(http_err):
        logger.warning("🔑 401 Unauthorized, getting new authentication token")
        get_authentication_token(authentication_url)
        if GlobalConfig().authentication_error_counter:
            GlobalConfig().authentication_error_counter.add(1, {"method": method_name})
    else:
        logger.error(f"❌ HTTP status error occurred: {http_err} - Response: {response.text}")
    if GlobalConfig().error_counter:
        GlobalConfig().error_counter.add(1, {"method": method_name, "url": url, "data": data})
    raise http_err
