import sys
import logging
from pipe_events.utils.parse import parse
from pipe_events.fishing_events_incremental import run as run_incremental
from pipe_events.fishing_events_auth_and_regions import run as run_auth_and_regions
from pipe_events.fishing_events_restricted import run as run_restricted
from pipe_events.utils.bigquery import BigqueryHelper


class Cli:
    def __init__(self, args):
        self._args = args
        self._log = logging.getLogger()
        self._bq = BigqueryHelper(args.project, self._log, args.test)

    @property
    def _params(self) -> dict:
        """ Command arguments to dict."""
        return vars(self._args)

    def _run_incremental_fishing_events(self) -> bool:
        return run_incremental(self._bq, self._params)

    def _run_auth_and_regions_fishing_events(self) -> bool:
        return run_auth_and_regions(self._bq, self._params)

    def _run_restricted_fishing_events(self) -> bool:
        return run_restricted(self._bq, self._params)

    def run(self) -> bool:
        """Executes the operation that matches."""
        result = False
        if self._args.operation == "incremental_events":
            result = self._run_incremental_fishing_events()
        elif self._args.operation == "auth_and_regions_fishing_events":
            result = self._run_auth_and_regions_fishing_events()
        elif self._args.operation == "fishing_restrictive":
            result = self._run_restricted_fishing_events()
        else:
            raise RuntimeError(f"Invalid operation: {self._args.operation}")
        return result


def main():
    """Executes the client."""
    cli = Cli(parse(sys.argv))
    result = cli.run()
    exit(0 if result else 1)


if __name__ == "__main__":
    main()
