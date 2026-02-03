import logging
from contextlib import asynccontextmanager
from typing import Callable, Optional

import sentry_sdk
from app_config import AppGlobalConfigBase, AppSettings
from dlq_endpoints import DLQConfig
from dlq_endpoints import router as dlq_router
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from global_config import GlobalConfig
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import PublisherOptions, SubscriberOptions
from health_endpoints import router as health_router
from init_opentelemetry import init_cloud
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prompt_endpoints import PromptsConfig
from prompt_endpoints import router as prompts_router
from pubsub_utils import setup_incoming_publisher
from sentry_sdk.integrations.fastapi import FastApiIntegration

logger = logging.getLogger(__name__)


def setup_sentry(settings: AppSettings):
    if settings.sentry_live:
        logger.info("🛡️ Initializing Sentry...")
        sentry_sdk.init(
            dsn=settings.sentry_url,
            traces_sample_rate=0,
            profiles_sample_rate=0,
            send_default_pii=True,
            environment=settings.env,
            integrations=[FastApiIntegration()],
        )
    else:
        logger.info("🚫 Sentry disabled")


def get_pubsub_topic_path(topic):
    return GlobalConfig().publisher.topic_path(AppSettings().gcp_project_id, topic)


def setup_outgoing_publisher(settings):
    try:
        logger.debug(f"📤 Initializing Incoming Publisher {settings.outgoing_topic}...")
        GlobalConfig().publisher = pubsub_v1.PublisherClient(publisher_options=PublisherOptions(enable_open_telemetry_tracing=True))
        GlobalConfig().outgoing_topic_path = GlobalConfig().publisher.topic_path(settings.gcp_project_id, settings.outgoing_topic)
        GlobalConfig().management_topic_path = GlobalConfig().publisher.topic_path(settings.gcp_project_id, settings.management_topic)

        logger.debug(f"✅ Incoming Publisher initialized {GlobalConfig().outgoing_topic_path}")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Incoming Publisher {GlobalConfig().outgoing_topic_path}, error: {str(e)}")
        raise


def setup_incoming_subscriber(settings, message_callback):
    try:
        logger.debug(f"📥 Initializing Incoming Subscriber {settings.gcp_project_id}, {settings.incoming_topic}...")
        GlobalConfig().subscriber = pubsub_v1.SubscriberClient(subscriber_options=SubscriberOptions(enable_open_telemetry_tracing=True))
        GlobalConfig().incoming_topic_path = GlobalConfig().subscriber.subscription_path(settings.gcp_project_id, settings.incoming_topic)
        GlobalConfig().publisher = pubsub_v1.PublisherClient(publisher_options=PublisherOptions(enable_open_telemetry_tracing=True))
        GlobalConfig().incoming_replay_topic_path = GlobalConfig().publisher.topic_path(
            settings.gcp_project_id, settings.incoming_topic.replace("-sub", "")
        )

        setup_incoming_publisher(
            settings.gcp_project_id,
            settings.incoming_topic,
            settings.max_messages,
            GlobalConfig().subscriber,
            message_callback,
        )

    except Exception as e:
        logger.error(f"❌ Failed to initialize Incoming Subscriber {{settings.incoming_topic}}, error: {str(e)}")
        raise


def setup_dql(settings):
    try:
        logger.debug(f"📥 Initializing DLQ Subscriber {settings.gcp_project_id}...")
        dlq_subscription = f"projects/{settings.gcp_project_id}/subscriptions/{settings.dlq_subscription}"  # DLQ subscription
        subscriber = pubsub_v1.SubscriberClient(subscriber_options=SubscriberOptions(enable_open_telemetry_tracing=True))
        GlobalConfig().subscriber = subscriber
        GlobalConfig().dlq_subscription = dlq_subscription
        logger.debug(f"✅ DLQ Sub client initialized. Topic path: {GlobalConfig().dlq_subscription}")
    except Exception as e:
        logger.error(f"❌ Failed to initialize DLQ Sub client{{settings.dlq_subscription}}, error: {str(e)}")
        raise


def shutdown_pubsub(error_counter):
    try:
        if GlobalConfig().subscriber:
            GlobalConfig().subscriber.close()
            logger.debug("🔒 Pub/Sub subscriber closed")
        if GlobalConfig().publisher:
            GlobalConfig().publisher.stop()
            logger.debug("🔒 Pub/Sub publisher closed")
    except Exception as e:
        if error_counter:
            error_counter.add(1, {"method": "POST", "path": "lifespan"})
        logger.error(f"❌ Error during cleanup: {str(e)}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Initializing GCP Pub/Sub client via lifespan...")
    settings = app.state.settings
    # init_cloud()
    setup_sentry(settings)
    setup_outgoing_publisher(settings)
    logging.getLogger("google.api_core.bidi").setLevel(logging.WARNING)

    setup_dql(settings)

    if app.state.message_callback:
        setup_incoming_subscriber(settings, app.state.message_callback)

    if app.state.app_init:
        app.state.app_init()
    if app.state.async_app_init:
        await app.state.async_app_init()

    yield

    shutdown_pubsub(GlobalConfig().error_counter)


def create_app(
    settings: AppSettings,
    app_config: Optional[AppGlobalConfigBase],
    message_callback: Optional[Callable] = None,
    app_init: Optional[Callable] = None,
    async_app_init: Optional[Callable] = None,
):
    init_cloud()
    logger.info(f"Loading git version: {settings.git_commit}")
    app = FastAPI(lifespan=lifespan, docs_url="/swagger", root_path=f"/{settings.service_name}")
    app.state.settings = settings
    app.state.app_init = app_init
    app.state.async_app_init = async_app_init
    app.state.app_config = app_config
    app.state.message_callback = message_callback

    @app.get("/")
    def read_root():
        return {"message": f"Welcome to the {settings.app_name}!"}

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon():
        """Handle favicon requests to prevent 404 errors."""
        return Response(status_code=204)

    app.include_router(health_router)
    if app_config:
        logger.debug(f"Including prompts router with app config: {app_config}")
        PromptsConfig(app_config).setup_hooks()
        app.include_router(prompts_router)
    logger.debug(f"Setting up DLQ with message callback: {message_callback}")
    DLQConfig().setup_hooks(message_callback)
    logger.debug("Including DLQ router")
    app.include_router(dlq_router)

    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r"https://.*\.(ourritual|heyritual|ritual-app)\.(com|co)",
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Allowed hosts for swagger/docs access (IAP-protected domain only)
    SWAGGER_ALLOWED_HOSTS = [
        "swagger.dev.ourritual.com",
        "swagger.ourritual.com",
        "localhost",
        "127.0.0.1",
    ]

    @app.middleware("http")
    async def swagger_security_middleware(request: Request, call_next):
        """
        Block access to /swagger, /docs, /redoc, /openapi.json on non-swagger domains.

        This ensures that API documentation is ONLY accessible via swagger.dev.ourritual.com
        which is protected by GCP IAP, not via public endpoints like webhooks.dev.ourritual.com.

        Security: Defense-in-depth approach with app-level host validation + GCP IAP.
        """
        path = request.url.path.lower()

        # Check if accessing swagger/docs endpoints
        swagger_paths = ["/swagger", "/docs", "/redoc", "/openapi.json"]
        is_swagger_path = any(path.startswith(p) for p in swagger_paths)

        if is_swagger_path:
            # Get the Host header
            host = request.headers.get("host", "").lower()

            # Remove port if present (e.g., localhost:8080 -> localhost)
            host_without_port = host.split(":")[0]

            # Check if host is allowed
            if host_without_port not in SWAGGER_ALLOWED_HOSTS:
                logger.warning(f"🚫 Blocked swagger access from unauthorized host: {host} " f"(path: {path})")
                return JSONResponse(
                    status_code=403,
                    content={"detail": "Swagger/API documentation access is restricted. " "Please use swagger.dev.ourritual.com"},
                )

        # Continue to the actual endpoint
        response = await call_next(request)
        return response

    if AppSettings().enable_app_instrumentation:
        logger.info("🔧 Instrumenting FastAPI app...")
        FastAPIInstrumentor.instrument_app(app, excluded_urls="ready,health")
    else:
        logger.info("🚫 FastAPI app instrumentation disabled")
    return app
