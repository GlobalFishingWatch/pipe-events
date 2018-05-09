from pipe_events.transforms.source import Source
from pipe_events.transforms.cleanup import Cleanup
from pipe_events.transforms.group import GroupByIdAndTimeBucket
from pipe_events.transforms.events import Events
from pipe_events.transforms.sink import Sink
from apache_beam import io

class PipelineDefinition():
    def __init__(self, options):
        self.options = options

    def build(self, pipeline):
        source = (
            pipeline
            | "ReadFromSource" >> Source(self.options.source)
        )

        events = (
            source
            | "Cleanup" >> Cleanup()
            | "GroupHourly" >> GroupByIdAndTimeBucket(GroupByIdAndTimeBucket.HOURLY_BUCKET)
            | "CollectEvents" >> Events(fishing_threshold=self.options.fishing_threshold)
        )

        if self.options.local:
            sink = io.WriteToText('output/events')
        elif self.options.remote:
            sink = Sink(
                table=self.options.sink_prefix,
                write_disposition=self.options.sink_write_disposition,
            )

        (
            events
            | "WriteToSink" >> sink
        )

        return pipeline