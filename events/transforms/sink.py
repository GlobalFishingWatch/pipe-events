from apache_beam import PTransform
from apache_beam import Map
from apache_beam import io

class Sink(PTransform):
    def __init__(self, table=None, write_disposition=None):
        self.table = table
        self.write_disposition = write_disposition

    def expand(self, xs):
        def encode_datetimes_to_iso(x):
            def encode_datetime_field(value):
                return value.strftime('%Y-%m-%d %H:%M:%S.%f UTC')

            for field in ['start', 'end']:
                x[field] = encode_datetime_field(x[field])

            return x

        def build_table_schema(spec):
            schema = io.gcp.internal.clients.bigquery.TableSchema()

            for name, type in spec.iteritems():
                field = io.gcp.internal.clients.bigquery.TableFieldSchema()
                field.name = name
                field.type = type
                field.mode = 'nullable'
                schema.fields.append(field)

            return schema

        return xs | Map(encode_datetimes_to_iso) | io.Write(io.gcp.bigquery.BigQuerySink(
            table=self.table,
            write_disposition=self.write_disposition,
            schema=build_table_schema({
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
        ))
