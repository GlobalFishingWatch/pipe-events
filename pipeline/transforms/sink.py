from apache_beam import PTransform
from apache_beam import Map
from apache_beam import io

class Sink(PTransform):
    def __init__(self, table=None, write_disposition=None):
        self.table = table
        self.write_disposition = write_disposition

    def encode_datetime(self, value):
        return value.strftime('%Y-%m-%d %H:%M:%S.%f UTC')

    def encode_datetime_fields(self, x):
        for field in ['start', 'end']:
            x[field] = self.encode_datetime(x[field])
        return x

    def build_table_schema(self, spec):
        schema = io.gcp.internal.clients.bigquery.TableSchema()

        for field_name, field_type in spec.iteritems():
            field = io.gcp.internal.clients.bigquery.TableFieldSchema()
            field.name = field_name
            field.type = field_type
            field.mode = 'nullable'
            schema.fields.append(field)

        return schema

    def expand(self, xs):
        big_query_sink = io.gcp.bigquery.BigQuerySink(
            table=self.table,
            write_disposition=self.write_disposition,
            schema=self.build_table_schema({
                "id": "integer",
                "start": "timestamp",
                "end": "timestamp",
                "min_lat": "float",
                "min_lon": "float",
                "max_lat": "float",
                "max_lon": "float",
                "count": "integer",
                "score_avg_fishing": "float",
                "score_stddev_fishing": "float",
                "score_avg_all": "float",
                "score_stddev_all": "float",
            })
        )

        return (
            xs
            | Map(self.encode_datetime_fields)
            | io.Write(big_query_sink)
        )
