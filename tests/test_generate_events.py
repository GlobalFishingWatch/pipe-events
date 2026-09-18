import argparse
import unittest.mock as utm
from datetime import date

from pipe_events import encounter_events, loitering_events, port_visit_events
from pipe_events import fishing_events_auth_and_regions as auth_and_regions
from pipe_events import fishing_events_incremental_filter as incremental_filter


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
                "--bq-in-regions-registry", "p.d.registry",
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
        bq.fetch_rows.return_value = [{"name": "eez", "description": "Exclusive Economic Zones."}]
        with utm.patch.object(encounter_events, "publish_versioned_events") as pub:
            pub.return_value = True
            assert encounter_events.run(bq, params) is True

        _, kwargs = pub.call_args
        assert kwargs["dest_table"] == "p.d.dest"
        assert kwargs["end_date"] == date(2024, 1, 2)
        assert kwargs["sql_template"] == "encounter-events.sql.j2"
        assert kwargs["labels"] == {"step": "generate_events"}
        assert kwargs["regions"] == [{"name": "eez", "description": "Exclusive Economic Zones."}]
        assert kwargs["template_params"] == {
            "encounters_table": "p.d.enc",
            "spatial_measures_table": "p.d.sm",
            "regions_table": "p.d.reg",
            "product_vessel_info_summary_table": "p.d.pvis",
            "product_vessel_info_summary_field_prefix": "ais_",
            "product_vessel_info_summary_flag_field": "ais_mmsi_flag",
            "vessel_identity_core": "p.d.idcore",
            "vessel_identity_authorization": "p.d.idauth",
            "voyages_table": "p.d.voy",
            "port_visits_table": "p.d.pv",
            "regions": ["eez"],
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
                "--bq-in-regions-registry", "p.d.registry",
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
        assert kwargs["template_params"]["product_vessel_info_summary_flag_field"] == (
            "ais_mmsi_flag"
        )
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
                "--bq-in-regions-registry", "p.d.registry",
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
        assert kwargs["template_params"]["named_anchorages_dock_field"] == "at_dock"
        assert "start_date" not in kwargs["template_params"]

    def test_dock_field_is_overridable(self):
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
                "--bq-in-regions-registry", "p.d.registry",
                "--bq-in-named-anchorages", "p.d.anch",
                "--named-anchorages-dock-field", "dock",
                "--bq-out-events", "p.d.dest",
                "--labels", LABELS_ARG,
            ],
        )
        assert params["named_anchorages_dock_field"] == "dock"


class TestFlagField:
    """The PVIS field carrying the vessel flag is configurable (PIPELINE-4424).

    It defaults to `<field-prefix>mmsi_flag` — what every caller read before the
    argument existed — and VMS pipelines override it with `gfw_best_flag`.
    """

    ENCOUNTER_ARGS = [
        "--start-date", "2024-01-01",
        "--end-date", "2024-01-02",
        "--bq-in-encounters", "p.d.enc",
        "--bq-in-spatial-measures", "p.d.sm",
        "--bq-in-regions", "p.d.reg",
        "--bq-in-regions-registry", "p.d.registry",
        "--bq-in-product-vessel-info-summary", "p.d.pvis",
        "--product-vessel-info-summary-field-prefix", "self_reported_",
        "--bq-in-vessel-identity-core", "p.d.idcore",
        "--bq-in-vessel-identity-authorization", "p.d.idauth",
        "--bq-in-voyages", "p.d.voy",
        "--bq-in-port-visits", "p.d.pv",
        "--bq-out-events", "p.d.dest",
        "--labels", LABELS_ARG,
    ]

    PORT_VISIT_ARGS = [
        "--start-date", "2024-01-01",
        "--end-date", "2024-01-02",
        "--bq-in-port-visits", "p.d.pv",
        "--bq-in-product-vessel-info-summary", "p.d.pvis",
        "--product-vessel-info-summary-field-prefix", "self_reported_",
        "--bq-in-spatial-measures", "p.d.sm",
        "--bq-in-regions", "p.d.reg",
        "--bq-in-regions-registry", "p.d.registry",
        "--bq-in-named-anchorages", "p.d.anch",
        "--bq-out-events", "p.d.dest",
        "--labels", LABELS_ARG,
    ]

    def _flag_field(self, module, argv):
        params = _parse(module, argv)
        bq = utm.MagicMock()
        with utm.patch.object(module, "publish_versioned_events") as pub:
            pub.return_value = True
            module.run(bq, params)
        _, kwargs = pub.call_args
        return kwargs["template_params"]["product_vessel_info_summary_flag_field"]

    def test_encounter_defaults_to_prefixed_mmsi_flag(self):
        assert self._flag_field(encounter_events, self.ENCOUNTER_ARGS) == (
            "self_reported_mmsi_flag"
        )

    def test_encounter_override(self):
        argv = self.ENCOUNTER_ARGS + [
            "--product-vessel-info-summary-flag-field", "gfw_best_flag",
        ]
        assert self._flag_field(encounter_events, argv) == "gfw_best_flag"

    def test_port_visit_defaults_to_prefixed_mmsi_flag(self):
        assert self._flag_field(port_visit_events, self.PORT_VISIT_ARGS) == (
            "self_reported_mmsi_flag"
        )

    def test_port_visit_override(self):
        argv = self.PORT_VISIT_ARGS + [
            "--product-vessel-info-summary-flag-field", "gfw_best_flag",
        ]
        assert self._flag_field(port_visit_events, argv) == "gfw_best_flag"


class TestFishingFlagField:
    """Same argument on the two fishing steps that read the flag from the PVIS.

    These steps hand the whole parameter dict to the template, so the assertions
    look at what reached `format_query`.
    """

    FILTER_ARGS = [
        "--bq-in-segments-activity", "p.d.segsact",
        "--bq-in-segment-vessel", "p.d.segvessel",
        "--bq-in-product-vessel-info-summary", "p.d.pvis",
        "--product-vessel-info-summary-field-prefix", "self_reported_",
        "--score-field", "nnet_score",
        "--bq-in-udfs-dataset", "p.udfs",
        "--bq-out-filtered-events", "p.d.filtered",
        "--bq-in-merged-events", "p.d.merged",
        "--labels", LABELS_ARG,
    ]

    AUTH_ARGS = [
        "--bq-in-fishing-events", "p.d.fishing",
        "--bq-in-night-loitering-events", "p.d.nl",
        "--bq-in-vessel-identity-core", "p.d.idcore",
        "--bq-in-vessel-identity-authorization", "p.d.idauth",
        "--bq-in-spatial-measures", "p.d.sm",
        "--bq-in-regions", "p.d.reg",
        "--bq-in-regions-registry", "p.d.registry",
        "--bq-in-product-vessel-info-summary", "p.d.pvis",
        "--product-vessel-info-summary-field-prefix", "self_reported_",
        "--bq-in-nautical-time-raster", "p.d.nautical",
        "--bq-in-udfs-dataset", "p.udfs",
        "--bq-out-events", "p.d.dest",
        "--bq-out-events-view", "p.d.dest_view",
        "--reference-date", "2024-01-01",
        "--labels", LABELS_ARG,
    ]

    def _flag_field(self, module, argv):
        params = _parse(module, argv)
        params["base_table_description"] = ""
        params["table_description"] = ""
        bq = utm.MagicMock()
        assert module.run(bq, params) is True
        _, kwargs = bq.format_query.call_args
        return kwargs["product_vessel_info_summary_flag_field"]

    def test_incremental_filter_defaults_to_prefixed_mmsi_flag(self):
        assert self._flag_field(incremental_filter, self.FILTER_ARGS) == (
            "self_reported_mmsi_flag"
        )

    def test_incremental_filter_override(self):
        argv = self.FILTER_ARGS + [
            "--product-vessel-info-summary-flag-field", "gfw_best_flag",
        ]
        assert self._flag_field(incremental_filter, argv) == "gfw_best_flag"

    def test_auth_and_regions_defaults_to_prefixed_mmsi_flag(self):
        assert self._flag_field(auth_and_regions, self.AUTH_ARGS) == (
            "self_reported_mmsi_flag"
        )

    def test_auth_and_regions_override(self):
        argv = self.AUTH_ARGS + [
            "--product-vessel-info-summary-flag-field", "gfw_best_flag",
        ]
        assert self._flag_field(auth_and_regions, argv) == "gfw_best_flag"
