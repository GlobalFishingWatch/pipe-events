import pipe_events.cli as cli
import unittest.mock as utm
import pytest


class Args:

    project: str = ''
    operation: str = ''
    test: bool = True

    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])


class TestCli:

    @utm.patch("pipe_events.utils.bigquery.BigqueryHelper", autospec=True)
    @utm.patch("google.cloud.bigquery.Client", autospec=True)
    def test_cli(self, mocked, m2):
        cl = cli.Cli(Args())
        assert cl is not None

    @utm.patch("pipe_events.utils.bigquery.BigqueryHelper", autospec=True)
    @utm.patch("google.cloud.bigquery.Client", autospec=True)
    def test_unknown_operation_main(self, mocked, m2):
        with pytest.raises(SystemExit) as err:
            utm.MagicMock(return_value=Args())
            cli.main()
        assert err.value.code == 2

    @utm.patch("pipe_events.utils.bigquery.BigqueryHelper", autospec=True)
    @utm.patch("google.cloud.bigquery.Client", autospec=True)
    def test_invalid_operation_cli(self, mocked, m2):
        with pytest.raises(RuntimeError):
            cl = cli.Cli(Args())
            cl.run()

    @utm.patch("pipe_events.utils.bigquery.BigqueryHelper", autospec=True)
    @utm.patch("google.cloud.bigquery.Client", autospec=True)
    def test_valid_operation_cli(self, mocked, m2):
        cl = cli.Cli(Args(operation='incremental_events'))
        cl._run_incremental_fishing_events = utm.MagicMock(return_value=1)
        assert cl.run() == 1

        cl = cli.Cli(Args(operation='auth_and_regions_fishing_events'))
        cl._run_auth_and_regions_fishing_events = utm.MagicMock(return_value=2)
        assert cl.run() == 2

        cl = cli.Cli(Args(operation='restricted_view_events'))
        cl._run_restricted_view_fishing_events = utm.MagicMock(return_value=3)
        assert cl.run() == 3

    @utm.patch("pipe_events.utils.bigquery.BigqueryHelper", autospec=True)
    @utm.patch("google.cloud.bigquery.Client", autospec=True)
    def test_params(self, mocked, m2):
        cl = cli.Cli(Args(operation='incremental_events'))
        assert cl._params == {'operation': 'incremental_events'}
