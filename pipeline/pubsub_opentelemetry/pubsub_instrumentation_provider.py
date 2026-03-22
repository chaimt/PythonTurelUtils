from __future__ import annotations

from typing import Callable, Collection, TypeVar

from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore
from opentelemetry.instrumentation.utils import unwrap
from wrapt import wrap_function_wrapper

from pubsub_opentelemetry.publisher_decorator import PublisherDecorator
from pubsub_opentelemetry.subscriber_decorator import SubscriberDecorator
from pubsub_opentelemetry.utils import _get_distro_version

T = TypeVar("T")


class PubSubInstrumentationProvider(BaseInstrumentor):
    """
    An instrumentor for GCP PubSub
    See `BaseInstrumentor`
    """

    @staticmethod
    def _instrument_subscribe(tracer: trace.Tracer):
        """
        Wrap the 'subscribe()' function with the instrumented decorator
        """

        def wrapper(wrapped: Callable[..., T], instance, args, kwargs):
            decorated_subscribe = SubscriberDecorator(tracer).decorate(wrapped)
            return decorated_subscribe(*args, **kwargs)

        wrap_function_wrapper(SubscriberClient, "subscribe", wrapper)

    @staticmethod
    def _instrument_publish(tracer: trace.Tracer):
        """
        Wrap the 'publish()' function with the instrumented decorator
        """

        def wrapper(wrapped: Callable[..., T], instance, args, kwargs):
            decorated_publish = PublisherDecorator(tracer).decorate(wrapped)
            return decorated_publish(*args, **kwargs)

        wrap_function_wrapper(PublisherClient, "publish", wrapper)

    def instrumentation_dependencies(self) -> Collection[str]:
        """
        The version of the instrumented library
        """
        return ["google-cloud-pubsub ~= 2.13"]

    def _instrument(self, **kwargs):
        """
        Instrument the PubSub module
        """
        tracer_provider = kwargs.get("tracer_provider")
        tracer = trace.get_tracer(
            __name__,
            _get_distro_version("google-cloud-pubsub"),
            tracer_provider=tracer_provider,
        )
        self._instrument_subscribe(tracer)
        self._instrument_publish(tracer)

    def _uninstrument(self, **kwargs):
        """
        Uninstrument the PubSub module
        """
        unwrap(SubscriberClient, "subscribe")
        unwrap(PublisherClient, "publish")
