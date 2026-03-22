import json
import logging
from typing import Callable, Optional

import httpx
from fastapi import APIRouter, HTTPException, Request
from global_config import GlobalConfig
from pubsub_utils import ErrorMessage, publish_message
from single_instance_metaclass import SingleInstanceMetaClass

logger = logging.getLogger(__name__)
router = APIRouter()


class MockPubSubMessage:
    def __init__(self, data: bytes):
        self.data = data

    def ack(self):
        logger.info("✅ Mock message acknowledged")


class DLQConfig(metaclass=SingleInstanceMetaClass):
    def __init__(self):
        self.url = "http://localhost:8000/webhook"
        self.headers = {"accept": "application/json"}
        self.http_replay = False
        self.hooks_setup = False

    def setup_hooks(self, message_callback: Optional[Callable] = None):
        if not self.hooks_setup:
            if self.http_replay:

                @router.post(
                    "/dlq/replay",
                    summary="Reprocess messages from DLQ via HTTP",
                    description="Re-sends messages from the Dead Letter Queue (DLQ) to the main topic using HTTP endpoint",
                    response_description="Status of the message reprocessing operation",
                    responses={
                        200: {
                            "description": "Successfully reprocessed messages",
                            "content": {"application/json": {"example": {"status": "success", "message": "No messages in DLQ"}}},
                        },
                        500: {
                            "description": "Internal server error",
                            "content": {"application/json": {"example": {"detail": "Error pulling messages from DLQ: <error_message>"}}},
                        },
                    },
                    tags=["DLQ"],
                )
                async def reprocess_message_http(request: Request, max_messages: int = 5):
                    """Re-send a message from the DLQ to the main topic http."""

                    try:
                        logger.info(f"🔄 Replaying {max_messages} messages from DLQ...")
                        response = read_dlq_messages(max_messages)

                        if not response.received_messages:
                            logger.info("No messages found in DLQ.")
                            return {"status": "success", "message": "No messages in DLQ"}

                        for msg in response.received_messages:
                            error_msg = ErrorMessage.model_validate_json(msg.message.data.decode("utf-8"))
                            async with httpx.AsyncClient() as client:
                                response = await client.post(DLQConfig().url, headers=DLQConfig().headers, content=error_msg.message.encode("utf-8"))
                            response.raise_for_status()
                            http_response = response.content.decode("utf-8")
                            logger.info(f"✅ Message reprocessed and published to /webhook: {http_response}")
                            acknowledge_messages([msg.ack_id])
                        return {"status": "success", "message": f"Successfully reprocessed {len(response.received_messages)} messages from DLQ"}
                    except Exception as e:
                        logger.error(f"❌ Error pulling messages from DLQ: {e}")
                        if GlobalConfig().error_counter:
                            GlobalConfig().error_counter.add(1, {"method": "GET", "path": "/dlq/messages"})
                        raise HTTPException(status_code=500, detail=f"Error pulling messages from DLQ: {e}") from e

            else:

                @router.post(
                    "/dlq/replay",
                    summary="Reprocess messages from DLQ via PubSub",
                    description="Re-sends messages from the Dead Letter Queue (DLQ) to the main topic using PubSub",
                    response_description="Status of the message reprocessing operation",
                    responses={
                        200: {
                            "description": "Successfully reprocessed messages",
                            "content": {"application/json": {"example": {"status": "success", "message": "No messages in DLQ"}}},
                        },
                        500: {
                            "description": "Internal server error",
                            "content": {"application/json": {"example": {"detail": "Error pulling messages from DLQ: <error_message>"}}},
                        },
                    },
                    tags=["DLQ"],
                )
                async def reprocess_message_queue(request: Request, max_messages: int = 5):
                    """Re-send a message from the DLQ to the main topic pubsub."""

                    try:
                        response = read_dlq_messages(max_messages)

                        if not response.received_messages:
                            logger.info("No messages found in DLQ.")
                            return {"status": "success", "message": "No messages in DLQ"}

                        for msg in response.received_messages:
                            message_wrapper = ErrorMessage.model_validate_json(msg.message.data.decode("utf-8"))
                            message_id = publish_message(
                                GlobalConfig().publisher, GlobalConfig().incoming_replay_topic_path, message_wrapper.message.encode("utf-8")
                            )
                            logger.info(f"✅ Message reprocessed and published to pubsub: {message_id}")

                            # logger.info(f"Message reprocessed and published to /webhook: {http_response}")
                            acknowledge_messages([msg.ack_id])
                        return {"status": "success", "message": f"Successfully reprocessed {len(response.received_messages)} messages from DLQ"}
                    except Exception as e:
                        logger.error(f"❌ Error pulling messages from DLQ: {e}")
                        if GlobalConfig().error_counter:
                            GlobalConfig().error_counter.add(1, {"method": "GET", "path": "/dlq/messages"})
                        raise HTTPException(status_code=500, detail=f"Error pulling messages from DLQ: {e}") from e

                if message_callback:

                    @router.post(
                        "/debug/message-callback",
                        tags=["Debug"],
                    )
                    async def debug_message_callback(request_data: dict):
                        """
                        Debug endpoint to test the message_callback function with custom data.

                        This endpoint allows you to inject a custom message payload and test the
                        message_callback function without needing an actual Pub/Sub subscription.

                        Expected payload format:
                        {
                            "prompts": [...],
                            "session_info": {...}
                        }
                        """
                        try:
                            # Convert the request data to JSON bytes to simulate Pub/Sub message
                            message_data = json.dumps(request_data).encode("utf-8")

                            # Create a mock Pub/Sub message
                            mock_message = MockPubSubMessage(message_data)

                            # Call the message_callback function
                            message_callback(mock_message)

                            return {"status": "success", "message": "Message callback executed successfully", "processed_data": request_data}

                        except Exception as e:
                            logger.error(f"🐛 Error in debug endpoint: {str(e)}")
                            raise HTTPException(status_code=500, detail=f"Error processing message: {str(e)}")

        self.hooks_setup = True


def get_dlq_subscription():
    return str(GlobalConfig().dlq_subscription) + "-sub"


def acknowledge_messages(ack_ids: list[str]):
    subscriber = GlobalConfig().subscriber
    dlq_subscription = get_dlq_subscription()
    logger.info(f"✅ Acknowledging messages with ACK IDs: {ack_ids}")
    return subscriber.acknowledge(
        request={
            "subscription": dlq_subscription,
            "ack_ids": ack_ids,
        }
    )


def read_dlq_messages(max_messages: int):
    subscriber = GlobalConfig().subscriber
    dlq_subscription = get_dlq_subscription()
    logger.info(f"📥 Pulling {max_messages} messages from DLQ {dlq_subscription}...")
    if subscriber is None:
        raise ValueError("Subscriber is not initialized")
    response = subscriber.pull(
        request={
            "subscription": dlq_subscription,
            "max_messages": max_messages,
        }
    )
    return response


@router.get(
    "/dlq/messages",
    summary="Retrieve messages from DLQ",
    description="Retrieves messages from the Dead Letter Queue (DLQ) with optional message limit",
    response_description="List of messages from the DLQ",
    responses={
        200: {
            "description": "Successfully retrieved messages",
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "messages": [{"message_id": "msg123", "data": "message content", "attributes": {"key": "value"}, "ack_id": "ack123"}],
                    }
                }
            },
        },
        500: {
            "description": "Internal server error",
            "content": {"application/json": {"example": {"detail": "Error pulling messages from DLQ: <error_message>"}}},
        },
    },
    tags=["DLQ"],
)
async def pull_dlq_messages(request: Request, max_messages: int = 5):
    """Retrieve messages from the DLQ on request."""
    try:
        logger.info(f"📥 Pulling {max_messages} messages from DLQ...")
        response = read_dlq_messages(max_messages)

        if not response.received_messages:
            logger.info("📭 No messages found in DLQ.")
            return {"status": "success", "message": "No messages in DLQ"}

        messages = [
            {
                "message_id": msg.message.message_id,
                "data": (msg.message.data or b"").decode("utf-8", errors="replace"),
                "attributes": dict(msg.message.attributes or {}),
                "ack_id": msg.ack_id,
            }
            for msg in response.received_messages
        ]

        logger.info(f"📦 Pulled {len(messages)} messages from DLQ.")
        return {"status": "success", "messages": messages}
    except Exception as e:
        logger.error(f"❌ Error pulling messages from DLQ: {e}")
        if GlobalConfig().error_counter:
            GlobalConfig().error_counter.add(1, {"method": "GET", "path": "/dlq/messages"})
        raise HTTPException(status_code=500, detail=f"Error pulling messages from DLQ: {e}") from e


@router.post(
    "/dlq/ack",
    summary="Acknowledge messages in DLQ",
    description="Acknowledges messages in the Dead Letter Queue (DLQ) by their acknowledgment IDs",
    response_description="Status of the acknowledgment operation",
    responses={
        200: {
            "description": "Successfully acknowledged messages",
            "content": {"application/json": {"example": {"status": "success", "message": "5 Messages acknowledged"}}},
        },
        500: {
            "description": "Internal server error",
            "content": {"application/json": {"example": {"detail": "Error acknowledging message: <error_message>"}}},
        },
    },
    tags=["DLQ"],
)
async def acknowledge_message(request: Request, max_messages: int = 5, ack_ids: list[str] | None = None):
    """Acknowledge a message in the DLQ."""
    logger.info(f"📥 Pulling {max_messages} messages from DLQ...")
    try:
        if not ack_ids:
            response = read_dlq_messages(max_messages)
            ack_ids = [msg.ack_id for msg in response.received_messages]
            if not ack_ids:
                logger.info("📭 No messages found in DLQ.")
                return {"status": "success", "message": "No messages in DLQ"}

        logger.info(f"✅ Acknowledging messages with ACK IDs: {ack_ids}")
        acknowledge_messages(ack_ids)
        logger.info(f"✅ {len(ack_ids)} Messages acknowledged successfully.")
        return {"status": "success", "message": f"{len(ack_ids)} Messages acknowledged"}
    except Exception as e:
        logger.error(f"❌ Error acknowledging message: {e}")
        if GlobalConfig().error_counter:
            GlobalConfig().error_counter.add(1, {"method": "POST", "path": "/dlq/ack"})
        raise HTTPException(status_code=500, detail=f"Error acknowledging message: {e}")


@router.get(
    "/dlq/info",
    summary="Display DLQ info",
    description="Get information about the Dead Letter Queue (DLQ) including message count",
    response_description="Json with DLQ info",
    responses={
        200: {
            "description": "Successfully retrieved DLQ info",
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "subscription": "projects/project/subscriptions/dlq-sub",
                        "message_count": 5,
                        "topic": "projects/project/topics/dlq-topic",
                    }
                }
            },
        },
        500: {
            "description": "Internal server error",
            "content": {"application/json": {"example": {"detail": "Error getting DLQ info: <error_message>"}}},
        },
    },
    tags=["DLQ"],
)
async def dlq_info(request: Request):
    """Get information about the DLQ including message count."""
    logger.info("ℹ️ Getting DLQ information...")
    try:
        subscriber = GlobalConfig().subscriber
        if subscriber is None:
            raise ValueError("Subscriber is not initialized")

        dlq_subscription = get_dlq_subscription()

        # Get subscription information including message count
        subscription_info = subscriber.get_subscription(request={"subscription": dlq_subscription})

        # Extract relevant information
        dlq_info = {
            "status": "success",
            "topic": str(subscription_info.topic),
            "subscription": dlq_subscription,
            "ack_deadline_seconds": str(subscription_info.ack_deadline_seconds),
            "message_retention_duration": str(subscription_info.message_retention_duration),
            "retain_acked_messages": str(subscription_info.retain_acked_messages),
            "retry_policy": str(subscription_info.retry_policy),
        }

        # logger.info(f"DLQ info retrieved: {dlq_info['message_count']} undelivered messages")
        return dlq_info

    except Exception as e:
        logger.error(f"❌ Error getting DLQ info: {e}")
        if GlobalConfig().error_counter:
            GlobalConfig().error_counter.add(1, {"method": "GET", "path": "/dlq/info"})
        raise HTTPException(status_code=500, detail=f"Error getting DLQ info: {e}")
