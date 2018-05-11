import pytest
import posixpath as pp
import newlinejson as nlj
import six
from copy import deepcopy

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
# rename the class to prevent py.test from trying to collect TestPipeline as a unit test class

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import open_shards

from pipe_tools.coders import JSONDictCoder
from pipe_tools.generator import MessageGenerator

import pipe_events
from pipe_events.__main__ import run as  pipe_events_run


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
@pytest.mark.filterwarnings('ignore:open_shards is experimental.:FutureWarning')
class TestPipeline():

    def test_placeholder(self):
        # TODO: replace this with a real test of the dataflow transforms
        assert True
