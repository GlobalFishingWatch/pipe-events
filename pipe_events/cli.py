import sys
import logging
from pipe_events.utils.parse import parse
from pipe_events.fishing_events_incremental import run as run_incremental
from pipe_events.fishing_events_auth_and_regions import run as run_auth_and_regions
from pipe_events.utils.bigquery import BigqueryHelper


class Cli:
    def __init__(self, args):
        self.args = args
        self.log = logging.getLogger()
        self.bq = BigqueryHelper(args.project, self.log, args.test)

    @property
    def params(self):
        return vars(self.args)

    def run_incremental_fishing_events(self):
        return run_incremental(self.bq, self.params)

    def run_auth_and_regions_fishing_events(self):
        return run_auth_and_regions(self.bq, self.params)

    def run(self):
        result = False
        if self.args.operation == "incremental_events":
            result = self.run_incremental_fishing_events()
        elif self.args.operation == "auth_and_regions_fishing_events":
            result = self.run_auth_and_regions_fishing_events()
        else:
            raise RuntimeError(f"Invalid operation: {self.args.operation}")
        return result


def main():
    cli = Cli(parse(sys.argv))
    result = cli.run()
    exit(0 if result else 1)


if __name__ == "__main__":
    main()
