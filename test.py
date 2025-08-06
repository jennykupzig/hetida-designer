import os

import logfire

# Jaeger only supports traces, not metrics, so only set the traces endpoint
# to avoid errors about failing to export metrics.
# Use port 4318 for HTTP, not 4317 for gRPC.
traces_endpoint = 'http://localhost:4318/v1/traces'
os.environ['OTEL_EXPORTER_OTLP_TRACES_ENDPOINT'] = traces_endpoint

logfire.configure(
    # Setting a service name is good practice in general, but especially
    # important for Jaeger, otherwise spans will be labeled as 'unknown_service'
    service_name='my_logfire_service',

    # Sending to Logfire is on by default regardless of the OTEL env vars.
    # Keep this line here if you don't want to send to both Jaeger and Logfire.
    send_to_logfire=False,
)

with logfire.span('This is a span'):
    logfire.info('Logfire logs are also actually just spans!')
