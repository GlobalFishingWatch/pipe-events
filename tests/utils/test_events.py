import unittest.mock as utm
from datetime import date

from pipe_events.constants import EVENTS_SCHEMA
from pipe_events.utils import events


class TestVersionedTableNames:
    def test_current_and_previous(self):
        current, previous = events.versioned_table_names("p.d.t", date(2024, 1, 1))
        assert current == "p.d.t_v20240101"
        # previous rolls back across the month/year boundary
        assert previous == "p.d.t_v20231231"


class TestBuildDescription:
    def test_includes_base_url_and_maintainers(self):
        desc = events.build_description(
            {"base_table_description": "Base", "table_description": "Extra"},
            "http://example/query.sql.j2",
            ["Data: a <a@x>", "Engineer: b <b@x>"],
        )
        assert "Base\nExtra" in desc
        assert "http://example/query.sql.j2" in desc
        assert "Data: a <a@x>" in desc
        assert "Engineer: b <b@x>" in desc


class TestPublishVersionedEvents:
    def test_runs_full_swap_in_order(self):
        bq = utm.MagicMock()
        bq.format_query.side_effect = lambda template, **kw: f"SQL:{template}"

        result = events.publish_versioned_events(
            bq,
            dest_table="p.d.events",
            end_date=date(2024, 3, 10),
            sql_template="encounter-events.sql.j2",
            template_params={"encounters_table": "p.d.enc"},
            description="the description",
            labels={"step": "generate_events"},
        )

        assert result is True
        current = "p.d.events_v20240310"
        previous = "p.d.events_v20240309"

        bq.create_table.assert_called_once_with(
            current,
            schema_file=EVENTS_SCHEMA,
            table_description="the description",
            partition_field="event_start",
            labels={"step": "generate_events"},
        )
        # truncate query is rendered and run without a destination
        bq.format_query.assert_any_call("truncate-table.sql.j2", table=current)
        bq.format_query.assert_any_call(
            "encounter-events.sql.j2", encounters_table="p.d.enc"
        )
        bq.run_query.assert_any_call("SQL:truncate-table.sql.j2")
        bq.run_query.assert_any_call(
            "SQL:encounter-events.sql.j2",
            dest_table=current,
            write_disposition="WRITE_APPEND",
            partition_field="event_start",
            labels={"step": "generate_events"},
        )
        bq.create_view.assert_called_once_with(
            "p.d.events",
            f"SELECT * FROM `{current}`",
            "the description",
            {"step": "generate_events"},
        )
        bq.update_table_schema.assert_called_once_with("p.d.events", EVENTS_SCHEMA)
        bq.remove_table.assert_called_once_with(previous)
