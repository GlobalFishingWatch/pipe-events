from apache_beam.options.pipeline_options import PipelineOptions

class EventsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--source',
            help='BigQuery query that returns the records to process. Might be either a query or a file containing the query if using the `@path/to/file.sql syntax`. Requires selecting the following fields: lat:float, long:float, score:float, timestamp:timestamp, id:integer. See examples/source.sql.',
        )

        parser.add_argument(
            '--sink',
            help='Prefix for the tileset BigQuery table names to which the processed data is uploaded. The actual event data will be uploaded to different [PREFIX]_[GRANULARITY] tables, where granularity might be `hours` or `days`',
            required=True,
        )

        parser.add_argument(
            '--sink_write_disposition',
            help='How to merge the output of this process with whatever records are already there in the sink tables. Might be WRITE_TRUNCATE to remove all existing data and write the new data, or WRITE_APPEND to add the new date without.',
            default='WRITE_APPEND',
        )

        parser.add_argument(
            '--fishing_threshold',
            help='Score threshold to consider a message to be a fishing event',
            type=float,
            default=0.5,
        )
