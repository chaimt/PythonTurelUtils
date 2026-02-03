import logging

from const import OURRITUAL_CONTEXT_HEADER
from global_config import GlobalConfig
from google.cloud import pubsub_v1
from log_helper import OurRitualContext
from opentelemetry.propagate import inject
from opentelemetry.trace import SpanKind
from pipeline_utils import wrap_message_callback
from pydantic import BaseModel
from retry_policy import RETRY_POLICY

logger = logging.getLogger(__name__)


def setup_incoming_publisher(gcp_project_id, incoming_topic, max_messages, subscriber, message_callback):
    # Wrap the message callback with trace context handling
    wrapped_callback = wrap_message_callback(message_callback)
    flow_control = pubsub_v1.types.FlowControl(max_messages=max_messages)
    incoming_topic_path = subscriber.subscription_path(gcp_project_id, incoming_topic)
    subscriber.subscribe(incoming_topic_path, callback=wrapped_callback, flow_control=flow_control)
    logger.debug(f"✅ Incoming Subscriber initialized {gcp_project_id}, {incoming_topic_path}")


class ErrorMessage(BaseModel):
    message: str
    error: str


class AcknowledgeMessage(BaseModel):
    ack_id: str


class ReprocessMessage(BaseModel):
    message_id: str
    data: dict


@RETRY_POLICY
def publish_message(publisher, topic_path, message_json, attributes=None):
    with GlobalConfig().tracer.start_as_current_span(
        f"{GlobalConfig().settings.service_name}_publish", kind=SpanKind.PRODUCER, attributes={"service.name": GlobalConfig().settings.service_name}
    ):
        # Add trace context to message attributes
        if attributes is None:
            attributes = {}
        inject(attributes)
        attributes[OURRITUAL_CONTEXT_HEADER] = OurRitualContext().to_header()
        # Publish with trace context attributes
        future = publisher.publish(topic_path, message_json, **attributes)
        return future.result()


@RETRY_POLICY
def publish_to_dlq(project_id: str, dlq_topic: str, data: bytes, error: Exception) -> str:
    """Publish message to DLQ with retry logic."""
    dlq_topic_path = GlobalConfig().publisher.topic_path(project_id, dlq_topic)
    message_wrapper = ErrorMessage(message=data, error=str(error)).model_dump_json().encode("utf-8")
    future = GlobalConfig().publisher.publish(dlq_topic_path, message_wrapper)
    res = future.result()
    logger.info(f"📤 Published message to DLQ {dlq_topic_path}")
    return res
