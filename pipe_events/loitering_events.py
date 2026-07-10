import json

from pipe_events.utils.events import build_description, publish_versioned_events
from pipe_events.utils.validators import valid_date, valid_table

COMMAND = "loitering_events"
HELP = "Publish loitering events."

SQL_TEMPLATE = "loitering-events.sql.j2"
SQL_URL = (
    "https://github.com/GlobalFishingWatch/pipe-events/blob/main/"
    "assets/bigquery/loitering-events.sql.j2"
)
MAINTAINERS = [
    "Data: Willa Brooks <willa@globalfishingwatch.org>",
    "Engineer: Álvaro Perdiz <alvaro@globalfishingwatch.org>",
]


def add_arguments(parser):
    parser.add_argument(
        "--start-date",
        dest="start_date",
        type=valid_date,
        required=True,
        help="Start date of the source range.",
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        type=valid_date,
        required=True,
        help="End date of the source range. Drives the versioned table name.",
    )
    parser.add_argument(
        "--bq-in-loitering",
        dest="source_loitering",
        type=valid_table,
        required=True,
        help="Source loitering table.",
    )
    parser.add_argument(
        "--bq-in-segment-info",
        dest="source_segment_info",
        type=valid_table,
        required=True,
        help="Source segment info table.",
    )
    parser.add_argument(
        "--bq-in-spatial-measures",
        dest="source_spatial_measures",
        type=valid_table,
        required=True,
        help="Source spatial measures table.",
    )
    parser.add_argument(
        "--bq-in-regions",
        dest="source_regions_table",
        type=valid_table,
        required=True,
        help="Source event regions table.",
    )
    parser.add_argument(
        "--bq-in-research-segments",
        dest="source_research_segs",
        type=valid_table,
        required=True,
        help="Source research segments table.",
    )
    parser.add_argument(
        "--bq-in-product-vessel-info-summary",
        dest="product_vessel_info_summary_table",
        type=valid_table,
        required=True,
        help="Product vessel info summary table.",
    )
    parser.add_argument(
        "--product-vessel-info-summary-field-prefix",
        dest="product_vessel_info_summary_field_prefix",
        type=str,
        required=True,
        help="Prefix to access vessel info fields in the PVIS table (e.g. 'ais_').",
    )
    parser.add_argument(
        "--minimum-distance-from-shore-nm",
        dest="minimum_distance_from_shore_nm",
        type=float,
        required=True,
        help="Minimum distance from shore, in nautical miles.",
    )
    parser.add_argument(
        "--bq-in-voyages",
        dest="voyages_table",
        type=valid_table,
        required=True,
        help="Voyages table.",
    )
    parser.add_argument(
        "--bq-in-port-visits",
        dest="port_visits_table",
        type=valid_table,
        required=True,
        help="Port visits table.",
    )
    parser.add_argument(
        "--bq-out-events",
        dest="dest_table",
        type=valid_table,
        required=True,
        help="Destination table. The versioned table and view derive from this.",
    )
    parser.add_argument(
        "--labels",
        type=json.loads,
        required=True,
        help="The labels assigned to each table.",
    )


def run(bq, params):
    template_params = {
        "start_date": params["start_date"].strftime("%Y-%m-%d"),
        "source_loitering": params["source_loitering"],
        "source_segment_info": params["source_segment_info"],
        "source_spatial_measures": params["source_spatial_measures"],
        "source_regions_table": params["source_regions_table"],
        "source_research_segs": params["source_research_segs"],
        "product_vessel_info_summary_table": params["product_vessel_info_summary_table"],
        "product_vessel_info_summary_field_prefix": params[
            "product_vessel_info_summary_field_prefix"
        ],
        "minimum_distance_from_shore_nm": params["minimum_distance_from_shore_nm"],
        "voyages_table": params["voyages_table"],
        "port_visits_table": params["port_visits_table"],
    }
    return publish_versioned_events(
        bq,
        dest_table=params["dest_table"],
        end_date=params["end_date"],
        sql_template=SQL_TEMPLATE,
        template_params=template_params,
        description=build_description(params, SQL_URL, MAINTAINERS),
        labels=params["labels"],
    )
