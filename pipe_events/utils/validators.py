import argparse
import datetime
import re


def valid_date(s: str) -> datetime.date:
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a valid date: {s!r}")


TABLE_REGEX = r"[\w\-_]+\.[\w\-_]+\.[\w\-_]+"


def valid_table(s: str) -> str:
    matched = re.fullmatch(TABLE_REGEX, s)
    if matched is None:
        raise argparse.ArgumentTypeError(
            f"not a valid table pattern (Format allowed <{TABLE_REGEX}>): {s!r}"
        )
    return s
