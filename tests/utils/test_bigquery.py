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
