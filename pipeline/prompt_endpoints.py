import logging
from typing import Any, Dict

from app_config import AppGlobalConfigBase
from fastapi import APIRouter, HTTPException
from single_instance_metaclass import SingleInstanceMetaClass

logger = logging.getLogger(__name__)
router = APIRouter()


class PromptsConfig(metaclass=SingleInstanceMetaClass):
    def __init__(self, app_config: AppGlobalConfigBase):
        self.app_config = app_config
        self.hooks_setup = False

    def setup_hooks(self):
        if not self.hooks_setup:

            @router.post("/prompts/clear-and-reload-prompts")
            async def clear_and_reload_prompts() -> Dict[str, Any]:
                """
                Clear all cached Langfuse prompts and force reload from Langfuse.

                This endpoint clears all cached prompt data from memory, forcing the application
                to fetch fresh prompt data from Langfuse on the next request that uses prompts.

                Returns:
                    Dict containing operation status and details about cleared prompts
                """
                try:
                    # Clear all cached prompts
                    cleared_count = self.app_config.clear_all_prompts()

                    # Force reload by accessing each prompt property
                    reloaded_prompts: list[str] = []
                    self.app_config.add_prompts(reloaded_prompts)

                    response_data = {
                        "status": "success",
                        "message": "Successfully cleared and reloaded prompts from Langfuse",
                        "cleared_count": cleared_count,
                        "reloaded_prompts": reloaded_prompts,
                        "total_reloaded": len(reloaded_prompts),
                    }

                    logger.info(f"✅ Prompts cleared and reloaded successfully: {response_data}")

                    return response_data

                except Exception as e:
                    error_msg = f"Failed to clear and reload prompts: {str(e)}"
                    logger.error(f"❌ {error_msg}")
                    raise HTTPException(status_code=500, detail=error_msg)

        self.hooks_setup = True
