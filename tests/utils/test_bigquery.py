import unittest.mock as utm


class TestUtilsBigQuery:

    def test_dest_table_description(self):
        from pipe_events.utils.bigquery import dest_table_description
        assert "\n" == dest_table_description()
        assert "Base\ntest" == dest_table_description(
                base_table_description="Base",
                table_description="test"
            )

    def test_as_date_str(self):
        from pipe_events.utils.bigquery import as_date_str
        from datetime import datetime, date
        assert "2020" == as_date_str("2020")
        assert 2020 == as_date_str(2020)
        assert "2020-01-01" == as_date_str(datetime(2020, 1, 1))
        assert "2020-01-01" == as_date_str(date(2020, 1, 1))

    def test_format_query(self):
        from pipe_events.utils.bigquery import format_query
        template = "./assets/bigquery/fishing-events-5-restrictive.sql.j2"
        with open(template, "r") as f:
            lines = f.read()
            lines = lines.replace("{{ source_restrictive_events }}", "")
            assert lines.strip() == format_query(
                "fishing-events-5-restrictive.sql.j2",
                source_restrictive_events=''
            )

    def test_remove_table_deletes_not_found_ok(self):
        from pipe_events.utils.bigquery import BigqueryHelper
        with utm.patch("pipe_events.utils.bigquery.bigquery.Client"):
            bq = BigqueryHelper(project="p")
        bq.remove_table("p.d.t")
        bq.client.delete_table.assert_called_once()
        _, kwargs = bq.client.delete_table.call_args
        assert kwargs["not_found_ok"] is True

    def test_load_schema_reads_a_json_file(self):
        from pipe_events.utils.bigquery import load_schema
        schema = load_schema("./assets/bigquery/events.schema.json")
        assert isinstance(schema, list)
        assert all("name" in field for field in schema)

    def test_load_schema_passes_through_an_already_loaded_schema(self):
        """A caller building a schema dynamically (see pipe_events.utils.regions) hands it
        straight to BigqueryHelper.create_table/update_table_schema the same way as a file.
        """
        from pipe_events.utils.bigquery import load_schema
        schema = [{"name": "event_id", "type": "STRING", "mode": "NULLABLE"}]
        assert load_schema(schema) is schema

    def test_fetch_rows_runs_a_plain_read_only_query(self):
        from pipe_events.utils.bigquery import BigqueryHelper
        with utm.patch("pipe_events.utils.bigquery.bigquery.Client"):
            bq = BigqueryHelper(project="p")

        result = bq.fetch_rows("SELECT 1")

        bq.client.query.assert_called_once()
        args, kwargs = bq.client.query.call_args
        assert args[0] == "SELECT 1"
        assert kwargs["job_config"].dry_run is False
        assert result is bq.client.query.return_value.result.return_value

    def test_fetch_rows_always_runs_for_real_under_dry_run(self):
        """Even with `--dry-run`, this metadata read must actually execute.

        Otherwise it returns no rows (BigQuery dry runs never return data), starving callers
        like `pipe_events.utils.regions.fetch_regions_registry` that need real data to render
        a valid query -- breaking dry-run validation of a query that would otherwise succeed.
        """
        from pipe_events.utils.bigquery import BigqueryHelper
        with utm.patch("pipe_events.utils.bigquery.bigquery.Client"):
            bq = BigqueryHelper(project="p", dry_run=True)

        bq.fetch_rows("SELECT 1")

        _, kwargs = bq.client.query.call_args
        assert kwargs["job_config"].dry_run is False

    def _dry_run_helper(self):
        from pipe_events.utils.bigquery import BigqueryHelper
        with utm.patch("pipe_events.utils.bigquery.bigquery.Client"):
            return BigqueryHelper(project="p", dry_run=True)

    def test_dry_run_changes_nothing(self):
        bq = self._dry_run_helper()
        schema = [{"name": "event_id", "type": "STRING", "mode": "NULLABLE"}]

        assert bq.begin_session() is None
        bq.end_session(None)
        bq.create_table("p.d.t", schema_file=schema, table_description="")
        bq.create_view("p.d.v", "SELECT 1", "")
        bq.remove_table("p.d.t")
        bq.clear_table_partition("p.d.t", "timestamp", "2024-01-01", "2024-01-02")
        bq.update_table_schema("p.d.t", schema)
        bq.update_table_description("p.d.t", "")
        bq.update_labels("p.d.t", {})
        bq.update_require_partition_filter("p.d.t")

        assert bq.client.mock_calls == []

    def test_dry_run_query_is_only_validated(self):
        bq = self._dry_run_helper()

        assert bq.run_query("SELECT 1", dest_table="p.d.t") is None

        bq.client.query.assert_called_once()
        _, kwargs = bq.client.query.call_args
        assert kwargs["job_config"].dry_run is True
        bq.client.query.return_value.result.assert_not_called()

    def test_dry_run_query_rejection_does_not_raise(self):
        from google.api_core.exceptions import NotFound
        bq = self._dry_run_helper()
        bq.client.query.side_effect = NotFound("Table p.d.t was not found")

        assert bq.run_query("SELECT * FROM `p.d.t`") is None
