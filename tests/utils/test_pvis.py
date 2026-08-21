from pipe_events.utils.pvis import resolve_flag_field


class TestResolveFlagField:
    def test_defaults_to_prefixed_mmsi_flag(self):
        params = {"product_vessel_info_summary_field_prefix": "self_reported_"}
        assert resolve_flag_field(params) == "self_reported_mmsi_flag"

    def test_defaults_to_bare_mmsi_flag_without_prefix(self):
        params = {"product_vessel_info_summary_field_prefix": ""}
        assert resolve_flag_field(params) == "mmsi_flag"

    def test_explicit_field_wins_over_the_prefix(self):
        params = {
            "product_vessel_info_summary_field_prefix": "self_reported_",
            "product_vessel_info_summary_flag_field": "gfw_best_flag",
        }
        assert resolve_flag_field(params) == "gfw_best_flag"

    def test_unset_field_falls_back_to_the_default(self):
        # argparse leaves the optional argument as None when it is not passed.
        params = {
            "product_vessel_info_summary_field_prefix": "ais_",
            "product_vessel_info_summary_flag_field": None,
        }
        assert resolve_flag_field(params) == "ais_mmsi_flag"
