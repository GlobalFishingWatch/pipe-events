from importlib.metadata import version

PIPELINE_NAME = "pipe-events"
PIPELINE_VERSION = version(PIPELINE_NAME)
PIPELINE_DESCRIPTION = "Generate summarized event information"

EVENTS_SCHEMA = "./assets/bigquery/events.schema.json"
