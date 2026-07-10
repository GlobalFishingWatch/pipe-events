import argparse
import unittest.mock as utm
from datetime import date

from pipe_events import encounter_events, loitering_events, port_visit_events


def _parse(module, argv):
    parser = argparse.ArgumentParser()
    module.add_arguments(parser)
    return vars(parser.parse_args(argv))


LABELS_ARG = '{"step": "generate_events"}'


class TestEncounterEvents:
    def test_run_delegates_with_template_params(self):
        params = _parse(
            encounter_events,
            [
                "--start-date", "2024-01-01",
                "--end-date", "2024-01-02",
                "--bq-in-encounters", "p.d.enc",
                "--bq-in-spatial-measures", "p.d.sm",
                "--bq-in-regions", "p.d.reg",
                "--bq-in-product-vessel-info-summary", "p.d.pvis",
                "--product-vessel-info-summary-field-prefix", "ais_",
                "--bq-in-vessel-identity-core", "p.d.idcore",
                "--bq-in-vessel-identity-authorization", "p.d.idauth",
                "--bq-in-voyages", "p.d.voy",
                "--bq-in-port-visits", "p.d.pv",
                "--bq-out-events", "p.d.dest",
                "--labels", LABELS_ARG,
            ],
        )
        bq = utm.MagicMock()
        with utm.patch.object(encounter_events, "publish_versioned_events") as pub:
            pub.return_value = True
            assert encounter_events.run(bq, params) is True

        _, kwargs = pub.call_args
        assert kwargs["dest_table"] == "p.d.dest"
        assert kwargs["end_date"] == date(2024, 1, 2)
        assert kwargs["sql_template"] == "encounter-events.sql.j2"
        assert kwargs["labels"] == {"step": "generate_events"}
        assert kwargs["template_params"] == {
            "encounters_table": "p.d.enc",
            "spatial_measures_table": "p.d.sm",
            "regions_table": "p.d.reg",
            "product_vessel_info_summary_table": "p.d.pvis",
            "product_vessel_info_summary_field_prefix": "ais_",
            "vessel_identity_core": "p.d.idcore",
            "vessel_identity_authorization": "p.d.idauth",
            "voyages_table": "p.d.voy",
            "port_visits_table": "p.d.pv",
        }


class TestLoiteringEvents:
    def test_run_passes_start_date_string(self):
        params = _parse(
            loitering_events,
            [
                "--start-date", "2024-01-01",
                "--end-date", "2024-01-02",
                "--bq-in-loitering", "p.d.loit",
                "--bq-in-segment-info", "p.d.seg",
                "--bq-in-spatial-measures", "p.d.sm",
                "--bq-in-regions", "p.d.reg",
                "--bq-in-research-segments", "p.d.rsegs",
                "--bq-in-product-vessel-info-summary", "p.d.pvis",
                "--product-vessel-info-summary-field-prefix", "ais_",
                "--minimum-distance-from-shore-nm", "0.5",
                "--bq-in-voyages", "p.d.voy",
                "--bq-in-port-visits", "p.d.pv",
                "--bq-out-events", "p.d.dest",
                "--labels", LABELS_ARG,
            ],
        )
        bq = utm.MagicMock()
        with utm.patch.object(loitering_events, "publish_versioned_events") as pub:
            pub.return_value = True
            assert loitering_events.run(bq, params) is True

        _, kwargs = pub.call_args
        assert kwargs["sql_template"] == "loitering-events.sql.j2"
        assert kwargs["end_date"] == date(2024, 1, 2)
        assert kwargs["template_params"]["start_date"] == "2024-01-01"
        assert kwargs["template_params"]["minimum_distance_from_shore_nm"] == 0.5
        assert "end_date" not in kwargs["template_params"]


class TestPortVisitEvents:
    def test_run_passes_end_date_string(self):
        params = _parse(
            port_visit_events,
            [
                "--start-date", "2024-01-01",
                "--end-date", "2024-01-02",
                "--bq-in-port-visits", "p.d.pv",
                "--bq-in-product-vessel-info-summary", "p.d.pvis",
                "--product-vessel-info-summary-field-prefix", "ais_",
                "--bq-in-spatial-measures", "p.d.sm",
                "--bq-in-regions", "p.d.reg",
                "--bq-in-named-anchorages", "p.d.anch",
                "--bq-out-events", "p.d.dest",
                "--labels", LABELS_ARG,
            ],
        )
        bq = utm.MagicMock()
        with utm.patch.object(port_visit_events, "publish_versioned_events") as pub:
            pub.return_value = True
            assert port_visit_events.run(bq, params) is True

        _, kwargs = pub.call_args
        assert kwargs["sql_template"] == "port-visits-events-v2.sql.j2"
        assert kwargs["template_params"]["end_date"] == "2024-01-02"
        assert kwargs["template_params"]["named_anchorages_table"] == "p.d.anch"
        assert "start_date" not in kwargs["template_params"]
