from airflow_ext.gfw.models import DagFactory

PIPELINE = "pipe_events"

class PipelineEventsDagFactory(DagFactory):

    def __init__(self, **kwargs):
        super(PipelineEventsDagFactory, self).__init__(pipeline=PIPELINE, **kwargs)
