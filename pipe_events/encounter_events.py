import json

from pipe_events.utils.events import build_description, publish_versioned_events
from pipe_events.utils.validators import valid_date, valid_table

COMMAND = "encounter_events"
HELP = "Publish encounter events."

SQL_TEMPLATE = "encounter-events.sql.j2"
SQL_URL = (
    "https://github.com/GlobalFishingWatch/pipe-events/blob/main/"
    "assets/bigquery/encounter-events.sql.j2"
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
        "--bq-in-encounters",
        dest="encounters_table",
        type=valid_table,
        required=True,
        help="Source encounters table.",
    )
    parser.add_argument(
        "--bq-in-spatial-measures",
        dest="spatial_measures_table",
        type=valid_table,
        required=True,
        help="Spatial measures table.",
    )
    parser.add_argument(
        "--bq-in-regions",
        dest="regions_table",
        type=valid_table,
        required=True,
        help="Event regions table.",
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
        "--bq-in-vessel-identity-core",
        dest="vessel_identity_core",
        type=valid_table,
        required=True,
        help="Vessel identity core table.",
    )
    parser.add_argument(
        "--bq-in-vessel-identity-authorization",
        dest="vessel_identity_authorization",
        type=valid_table,
        required=True,
        help="Vessel identity authorization table.",
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
        "encounters_table": params["encounters_table"],
        "spatial_measures_table": params["spatial_measures_table"],
        "regions_table": params["regions_table"],
        "product_vessel_info_summary_table": params["product_vessel_info_summary_table"],
        "product_vessel_info_summary_field_prefix": params[
            "product_vessel_info_summary_field_prefix"
        ],
        "vessel_identity_core": params["vessel_identity_core"],
        "vessel_identity_authorization": params["vessel_identity_authorization"],
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
