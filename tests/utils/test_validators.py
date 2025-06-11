import pytest
from datetime import date
from pipe_events.utils.validators import valid_date, valid_table


class TestValidators:

    @pytest.mark.parametrize(
        "entry,expected",
        [
            ("2020-01-01", date(2020, 1, 1)),
            ("2024-02-29", date(2024, 2, 29)),
            pytest.param("test", "test", marks=pytest.mark.xfail),
        ]
    )
    def test_valid_date(self, entry, expected):
        assert expected == valid_date(entry)

    @pytest.mark.parametrize(
        "table,expected",
        [
            ("a.b.c", "a.b.c"),
            pytest.param("a:b.c", "a:b.c", marks=pytest.mark.xfail),
            ("a-x.b-y.c-z", "a-x.b-y.c-z"),
            pytest.param("a-b.c", "a-b.c", marks=pytest.mark.xfail),
            pytest.param("test", "test", marks=pytest.mark.xfail),
        ]
    )
    def test_table_valid(self, table, expected):
        assert expected == valid_table(table)
