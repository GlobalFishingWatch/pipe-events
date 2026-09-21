import unittest.mock as utm

from pipe_events.utils import regions


class TestFetchRegionsRegistry:
    def test_queries_the_given_table_and_returns_plain_dicts(self):
        bq = utm.MagicMock()
        bq.fetch_rows.return_value = [{"name": "eez", "description": "Exclusive Economic Zones."}]

        result = regions.fetch_regions_registry(bq, "p.d.registry")

        query = bq.fetch_rows.call_args.args[0]
        assert "SELECT name, description" in query
        assert "p.d.registry" in query
        assert result == [{"name": "eez", "description": "Exclusive Economic Zones."}]


class TestRegionsMeanPositionFields:
    def test_builds_one_repeated_string_field_per_region(self):
        rows = [
            {"name": "eez", "description": "Exclusive Economic Zones."},
            {"name": "imma", "description": "International Marine Mammal Areas."},
        ]

        assert regions.regions_mean_position_fields(rows) == [
            {
                "name": "eez",
                "type": "STRING",
                "mode": "REPEATED",
                "description": "Exclusive Economic Zones.",
            },
            {
                "name": "imma",
                "type": "STRING",
                "mode": "REPEATED",
                "description": "International Marine Mammal Areas.",
            },
        ]


class TestWithDynamicRegionsSchema:
    def test_replaces_regions_mean_position_fields_only(self):
        schema = [
            {"name": "event_id", "type": "STRING", "mode": "NULLABLE"},
            {
                "name": "regions_mean_position",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [{"name": "stale", "type": "STRING", "mode": "REPEATED"}],
            },
        ]
        rows = [{"name": "eez", "description": "Exclusive Economic Zones."}]

        result = regions.with_dynamic_regions_schema(schema, rows)

        assert result[0] == {"name": "event_id", "type": "STRING", "mode": "NULLABLE"}
        assert result[1]["fields"] == [
            {
                "name": "eez",
                "type": "STRING",
                "mode": "REPEATED",
                "description": "Exclusive Economic Zones.",
            },
        ]

    def test_does_not_mutate_the_original_schema(self):
        schema = [
            {
                "name": "regions_mean_position",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [{"name": "stale", "type": "STRING", "mode": "REPEATED"}],
            },
        ]

        regions.with_dynamic_regions_schema(schema, [{"name": "eez", "description": "..."}])

        assert schema[0]["fields"] == [{"name": "stale", "type": "STRING", "mode": "REPEATED"}]
