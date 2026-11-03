import argparse
import datetime
import re


def valid_date(s: str) -> datetime.date:
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a valid date: {s!r}")


TABLE_REGEX = r"[\w\-_]+\.[\w\-_]+\.[\w\-_]+"
DATASET_REGEX = r"[\w\-_]+\.[\w\-_]+"
COLUMN_REGEX = r"[A-Za-z_][A-Za-z0-9_]*"
# PVIS gear types are lowercase snake_case, with `|` joining compound classes.
GEARTYPE_REGEX = r"[a-z0-9_]+(\|[a-z0-9_]+)*"


def valid_table(s: str) -> str:
    matched = re.fullmatch(TABLE_REGEX, s)
    if matched is None:
        raise argparse.ArgumentTypeError(
            f"not a valid table pattern (Format allowed <{TABLE_REGEX}>): {s!r}"
        )
    return s


def valid_dataset(s: str) -> str:
    matched = re.fullmatch(DATASET_REGEX, s)
    if matched is None:
        raise argparse.ArgumentTypeError(
            f"not a valid dataset pattern (Format allowed <{DATASET_REGEX}>): {s!r}"
        )
    return s


def valid_column(s: str) -> str:
    # Column names are interpolated into the SQL, so only plain identifiers are allowed.
    matched = re.fullmatch(COLUMN_REGEX, s)
    if matched is None:
        raise argparse.ArgumentTypeError(
            f"not a valid column name (Format allowed <{COLUMN_REGEX}>): {s!r}"
        )
    return s


def valid_geartype(s: str) -> str:
    # Interpolated into the SQL as a string literal, so quotes and the like are refused.
    matched = re.fullmatch(GEARTYPE_REGEX, s)
    if matched is None:
        raise argparse.ArgumentTypeError(
            f"not a valid gear type (Format allowed <{GEARTYPE_REGEX}>): {s!r}"
        )
    return s
