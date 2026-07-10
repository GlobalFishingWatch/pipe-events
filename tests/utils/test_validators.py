import argparse
import pytest
from datetime import date
from pipe_events.utils.validators import valid_date, valid_dataset, valid_table


class TestValidators:

    @pytest.mark.parametrize(
        "entry,expected",
        [
            ("2020-01-01", date(2020, 1, 1)),
            ("2024-02-29", date(2024, 2, 29)),
        ]
    )
    def test_valid_date(self, entry, expected):
        assert expected == valid_date(entry)

    @pytest.mark.parametrize("entry", ["test"])
    def test_valid_date_rejects_invalid(self, entry):
        with pytest.raises(argparse.ArgumentTypeError):
            valid_date(entry)

    @pytest.mark.parametrize(
        "table,expected",
        [
            ("a.b.c", "a.b.c"),
            ("a-x.b-y.c-z", "a-x.b-y.c-z"),
        ]
    )
    def test_table_valid(self, table, expected):
        assert expected == valid_table(table)

    @pytest.mark.parametrize("table", ["a:b.c", "a-b.c", "test"])
    def test_table_valid_rejects_invalid(self, table):
        with pytest.raises(argparse.ArgumentTypeError):
            valid_table(table)

    @pytest.mark.parametrize(
        "dataset,expected",
        [
            ("a.b", "a.b"),
            ("a-x.b-y", "a-x.b-y"),
        ]
    )
    def test_dataset_valid(self, dataset, expected):
        assert expected == valid_dataset(dataset)

    @pytest.mark.parametrize("dataset", ["a.b.c", "a:b", "test"])
    def test_dataset_valid_rejects_invalid(self, dataset):
        with pytest.raises(argparse.ArgumentTypeError):
            valid_dataset(dataset)
