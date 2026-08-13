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

    def _bq_helper_with_query_ok(self, dry_run=False):
        """Build a BigqueryHelper whose client.query() returns a job whose
        error_result is explicitly None (default MagicMock is truthy, which
        would trip the error branch)."""
        from pipe_events.utils.bigquery import BigqueryHelper
        with utm.patch("pipe_events.utils.bigquery.bigquery.Client"):
            bq = BigqueryHelper(project="p", dry_run=dry_run)
        bq.client.query.return_value.error_result = None
        return bq

    def test_delete_from_date_issues_expected_sql(self):
        """The DELETE renders `DATE(<field>) >= '<from_date>'`. The DATE()
        cast (vs bare column reference) matches ``clear_table_partition``'s
        convention and lets the helper work on either a DATE or TIMESTAMP
        partition column without changing shape."""
        bq = self._bq_helper_with_query_ok()
        bq.delete_from_date(
            "p.d.aggregated_events",
            "event_end_date",
            "2026-01-15",
        )
        bq.client.query.assert_called_once()
        args, kwargs = bq.client.query.call_args
        sql = args[0]
        assert "DELETE FROM `p.d.aggregated_events`" in sql
        assert "DATE(event_end_date) >= '2026-01-15'" in sql
        # Labels flow into the QueryJobConfig -- absent → empty dict.
        assert kwargs["job_config"].labels == {}

    def test_delete_from_date_accepts_date_object(self):
        """Date/datetime inputs are normalised via as_date_str."""
        from datetime import date
        bq = self._bq_helper_with_query_ok()
        bq.delete_from_date("p.d.t", "event_end_date", date(2026, 1, 15))
        sql = bq.client.query.call_args[0][0]
        assert "'2026-01-15'" in sql

    def test_delete_from_date_forwards_labels(self):
        bq = self._bq_helper_with_query_ok()
        bq.delete_from_date(
            "p.d.t", "event_end_date", "2026-01-15",
            labels={"environment": "dev", "step": "fishing_events"},
        )
        cfg = bq.client.query.call_args.kwargs["job_config"]
        assert cfg.labels == {"environment": "dev", "step": "fishing_events"}

    def test_delete_from_date_dry_run_skips_client_query(self):
        """dry_run=True MUST NOT issue the destructive DELETE. Matches
        ``clear_table_partition``'s policy on the same class -- a helper
        whose job is deleting production rows defaulting to the opposite
        policy would be a footgun for orchestrator dry runs."""
        from pipe_events.utils.bigquery import BigqueryHelper
        with utm.patch("pipe_events.utils.bigquery.bigquery.Client"):
            bq = BigqueryHelper(project="p", dry_run=True)
        bq.delete_from_date("p.d.t", "event_end_date", "2026-01-15")
        bq.client.query.assert_not_called()

    def test_delete_from_date_raises_on_error(self):
        """A job.error_result payload must surface as RuntimeError so the
        step-3a orchestrator's incremental path fails fast instead of
        continuing to WRITE_APPEND on top of a partial DELETE."""
        import pytest
        from pipe_events.utils.bigquery import BigqueryHelper
        with utm.patch("pipe_events.utils.bigquery.bigquery.Client"):
            bq = BigqueryHelper(project="p")
        bq.client.query.return_value.error_result = {
            "reason": "invalidQuery",
            "message": "boom",
        }
        with pytest.raises(RuntimeError, match="invalidQuery: boom"):
            bq.delete_from_date("p.d.t", "event_end_date", "2026-01-15")
