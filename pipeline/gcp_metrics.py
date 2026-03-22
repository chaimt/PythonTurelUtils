from opentelemetry import metrics
from opentelemetry.exporter.google.cloud import GoogleCloudMetricsExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

# Configure the Google Cloud exporter
exporter = GoogleCloudMetricsExporter(project_id="your-gcp-project-id")

# Create the metric reader and provider
reader = PeriodicExportingMetricReader(exporter)
provider = MeterProvider(metric_readers=[reader])
metrics.set_meter_provider(provider)

# Get the meter instance
meter = metrics.get_meter(__name__)
