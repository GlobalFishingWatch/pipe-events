import argparse
import logging
import os
import sys

from pipe_events import encounter_events
from pipe_events import fishing_events_auth_and_regions as auth_and_regions
from pipe_events import fishing_events_incremental as incremental
from pipe_events import fishing_events_incremental_filter as incremental_filter
from pipe_events import fishing_events_restricted as restricted
from pipe_events import loitering_events, port_visit_events
from pipe_events.constants import (
    PIPELINE_DESCRIPTION,
    PIPELINE_NAME,
    PIPELINE_VERSION,
)
from pipe_events.utils.bigquery import BigqueryHelper

# Each command module owns its COMMAND name, HELP text, add_arguments() and run().
COMMAND_MODULES = [
    incremental,
    incremental_filter,
    auth_and_regions,
    restricted,
    encounter_events,
    loitering_events,
    port_visit_events,
]
COMMANDS = {module.COMMAND: module for module in COMMAND_MODULES}


def setup_logging(verbosity):
    base_loglevel = getattr(logging, (os.getenv("LOGLEVEL", "WARNING")).upper())
    verbosity = min(verbosity, 2)
    loglevel = base_loglevel - (verbosity * 10)
    logging.basicConfig(stream=sys.stdout, level=loglevel, format="%(message)s")


def add_global_arguments(parser):
    """Arguments valid for any subcommand (parsed before the subcommand token)."""
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode - print query and exit. Do not run queries",
        default=False,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        dest="verbosity",
        default=0,
        help="verbose output (repeat for increased verbosity)",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_const",
        const=-1,
        default=0,
        dest="verbosity",
        help="quiet output (show errors only)",
    )
    parser.add_argument(
        "--project",
        type=str,
        help="GCP project id",
        required=True,
    )
    parser.add_argument(
        "--table_description",
        type=str,
        help="Additional text to include in the output table description",
        default="",
    )


def build_parser():
    parser = argparse.ArgumentParser(
        description=f"{PIPELINE_NAME}:{PIPELINE_VERSION} - {PIPELINE_DESCRIPTION}"
    )
    add_global_arguments(parser)

    subparsers = parser.add_subparsers(dest="operation", required=True)
    for module in COMMAND_MODULES:
        subparser = subparsers.add_parser(module.COMMAND, help=module.HELP)
        module.add_arguments(subparser)

    return parser


def parse(arguments):
    args = build_parser().parse_args(arguments[1:])

    setup_logging(args.verbosity)
    log = logging.getLogger()

    args.base_table_description = (
        f"Pipeline: {PIPELINE_NAME}:v{PIPELINE_VERSION}\n"
        f"Description: {PIPELINE_DESCRIPTION}\n"
    )
    log.info(args.base_table_description)
    log.info("==========================")

    return args


class Cli:
    def __init__(self, args):
        self._params = vars(args)
        self._log = logging.getLogger()
        self._bq = BigqueryHelper(
            getattr(args, "project", None), self._log, getattr(args, "test", False)
        )

    def run(self) -> bool:
        """Executes the operation that matches."""
        operation = self._params.get("operation")
        if operation not in COMMANDS:
            raise RuntimeError(f"Invalid operation: {operation}")

        return COMMANDS[operation].run(self._bq, self._params)


def main():
    """Executes the client."""
    cli = Cli(parse(sys.argv))
    result = cli.run()
    exit(0 if result else 1)


if __name__ == "__main__":
    main()
