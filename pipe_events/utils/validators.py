import argparse
import datetime
import re


def valid_date(s: str) -> datetime.date:
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a valid date: {s!r}")


def valid_table(s: str) -> str:
    matched = re.fullmatch(r"[\w\-_]+[:\.][\w\-_]+\.[\w\-_]+", s)
    if matched is None:
        raise argparse.ArgumentTypeError(
            f"not a valid table pattern (project.dataset.table): {s!r}"
        )
    return s
