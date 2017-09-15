import logging
from apache_beam import Pipeline
from apache_beam import io
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from options import EventsOptions
from events.transforms.source import Source
from events.transforms.cleanup import Cleanup
from events.transforms.group import GroupByIdAndTimeBucket
from events.transforms.events import Events
from events.transforms.sink import Sink

def run():
    options = PipelineOptions()
    google_options = options.view_as(GoogleCloudOptions)
    standard_options = options.view_as(StandardOptions)
    events_options = options.view_as(EventsOptions)

    logging.info("Running events pipeline with the following options")
    logging.info(options)
    logging.info(google_options)
    logging.info(standard_options)
    logging.info(events_options)

    query = Source.read_query(events_options.source)
    pipeline = Pipeline(options=options)

    (
        pipeline
        | "ReadFromSource" >> Source(query)
        | "Cleanup" >> Cleanup()
        | "GroupHourly" >> GroupByIdAndTimeBucket(GroupByIdAndTimeBucket.HOURLY_BUCKET)
        | "CollectEvents" >> Events(fishing_threshold=events_options.fishing_threshold)
        | "WriteToSink" >> Sink(
            table=events_options.sink,
            write_disposition=events_options.sink_write_disposition
        )
    )

    pipeline.run()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
