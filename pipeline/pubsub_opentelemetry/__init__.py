"""
This package will monkey patch the subscribe & publish functionality
within the PubSub library in order to instrument tracing.
.. _boto: https://pypi.org/project/google-cloud-pubsub/
Usage
-----
.. code:: python
    from google.cloud import pubsub_v1
    from pubsub_opentelemetry import PubSubInstrumentationProvider


    # Instrument PubSub
    PubSubInstrumentationProvider().instrument()

    # Publish creates a span with PubSub specific attributes
    with pubsub_v1.PublisherClient() as publisher:
        publisher.publish(topic_name, b'My first message!')

    # Subscribe propagates the context from the received message
    with pubsub_v1.SubscriberClient() as subscriber:
        subscriber.subscribe(subscription_name, callback)
API
---
"""

from .pubsub_instrumentation_provider import PubSubInstrumentationProvider
from .version import __version__

__all__ = ["PubSubInstrumentationProvider", "__version__"]
