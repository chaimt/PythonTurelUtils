from __future__ import annotations

from typing import Callable

from opentelemetry import trace
from opentelemetry.semconv.trace import SpanAttributes


class PublisherDecorator:
    """
    Apply the PublisherClient decorator
    """

    def __init__(self, tracer: trace.Tracer):
        self._tracer = tracer

    def decorate(self, publish: Callable) -> Callable:
        """
        Decorate the 'publish()' function, start a new span
        """

        def _decorated_publish(topic: str, data: bytes, *args, **kwargs):
            attributes = {
                SpanAttributes.MESSAGING_SYSTEM: "pubsub",
                SpanAttributes.MESSAGING_OPERATION: "send",
                SpanAttributes.MESSAGING_DESTINATION: topic,
                SpanAttributes.MESSAGING_DESTINATION_KIND: "topic",
                SpanAttributes.MESSAGING_PROTOCOL: "pubsub",
                SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: str(len(data)),
            }
            with self._tracer.start_as_current_span(f"{topic} send", kind=trace.SpanKind.PRODUCER, attributes=attributes):
                return publish(topic, data, *args, **kwargs)

        return _decorated_publish
