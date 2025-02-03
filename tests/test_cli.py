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

    @utm.patch('pipe_events.fishing_events_incremental.run')
    @utm.patch('pipe_events.fishing_events_auth_and_regions.run')
    @utm.patch('pipe_events.fishing_events_restricted.run')
    def test_run_incremental_fishing_events(self, m1, m2, m3):
        m1.return_value = True
        m2.return_value = True
        m3.return_value = True

        from pipe_events.cli import Cli

        cli_test = Cli(Args(operation='incremental_events'))
        assert cli_test._params == {'operation': 'incremental_events'}
        assert cli_test.run() is True

        cli_test = Cli(Args(operation='auth_and_regions_fishing_events'))
        assert cli_test._params == {'operation': 'auth_and_regions_fishing_events'}
        assert cli_test.run() is True

        cli_test = Cli(Args(operation='fishing_restrictive'))
        assert cli_test._params == {'operation': 'fishing_restrictive'}
        assert cli_test.run() is True

    def test_run_auth_and_regions_fishing_events(self):
        from pipe_events.cli import Cli
        cli_test = Cli(Args(operation='auth_and_regions_fishing_events'))
        cli_test._run_auth_and_regions_fishing_events = utm.MagicMock(return_value=2)
        assert cli_test._params['operation'] == 'auth_and_regions_fishing_events'
        assert cli_test.run() == 2

    def test_run_restrictive_fishing_events(self):
        from pipe_events.cli import Cli
        cli_test = Cli(Args(operation='fishing_restrictive'))
        cli_test._run_restricted_fishing_events = utm.MagicMock(return_value=3)
        assert cli_test._params['operation'] == 'fishing_restrictive'
        assert cli_test.run() == 3

    def test_cli_not_none(self):
        from pipe_events.cli import Cli
        cl = Cli(Args())
        assert cl is not None

    def test_run_invalid_operation(self):
        from pipe_events.cli import Cli
        cli_test = Cli(Args())
        with pytest.raises(RuntimeError):
            result = cli_test.run()
            assert result is not None
            assert result is False

    def test_unknown_operation_main(self):
        import pipe_events.cli as cli
        with pytest.raises(SystemExit) as err:
            cli.main()
        assert err.value.code == 2
