import csv
import importlib
import io
import pytest
import sys


class CSVWriterMock:
    def writer(self, csvfile):
        return self

    def writerow(self, chain):
        print(','.join(chain))


@pytest.mark.parametrize(
    "test_input,expected",
    [('{"event_id":"c51d4225939ae275c43de1d1a7f71c7a","event_type":"port_visit","vessel_id":"3c6ef7572-2a5f-2f78-21dd-b8afe9293891","event_start":"2018-12-22 18:59:00.000000 UTC","event_end":"2018-12-25 00:02:41.000000 UTC","lat_mean":"-23.6235361111","lon_mean":"-178.93359500003334","event_geography":"(-23.6235361111,-178.93359500003334)","event_info":"test","event_vessels":"[{\\"id\\":\\"3c6ef7572-2a5f-2f78-21dd-b8afe9293891\\",\\"ssvid\\":\\"247067640\\",\\"name\\":\\"Y2K\\",\\"type\\":\\"passenger\\",\\"flag\\":\\"ITA\\"}]"}', 'c51d4225939ae275c43de1d1a7f71c7a,port_visit,3c6ef7572-2a5f-2f78-21dd-b8afe9293891,2018-12-22 18:59:00.000000 UTC,2018-12-25 00:02:41.000000 UTC,test,[{"id":"3c6ef7572-2a5f-2f78-21dd-b8afe9293891","ssvid":"247067640","name":"Y2K","type":"passenger","flag":"ITA"}],MULTIPOINT(-23.6235361111,-178.93359500003334),POINT(-178.93359500003334 -23.6235361111)')] # noqa: E501 E261
)
def test_formatter(test_input, expected,  monkeypatch: pytest.MonkeyPatch):
    modname = "formatter"
    sys.argv = [modname, '/tmp/test.csv']
    mocked_stdin = io.StringIO(test_input)
    mocked_stdout = io.StringIO()

    with monkeypatch.context() as m:
        csv_writer_mock = CSVWriterMock()
        m.setattr(csv, 'writer', csv_writer_mock.writer)
        m.setattr(sys, 'stdin', mocked_stdin)
        m.setattr(sys, 'stdout', mocked_stdout)

        # load the module
        spec = importlib.util.spec_from_file_location(modname, "pipe_events/postgis/formatter.py")
        sys.modules[modname] = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(sys.modules[modname])

        assert mocked_stdout.getvalue().strip() == expected
