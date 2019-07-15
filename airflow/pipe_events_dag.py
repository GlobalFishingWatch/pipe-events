from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow_ext.gfw.models import DagFactory

import imp
import posixpath as pp

PIPELINE = "pipe_events"

subpipelines = {
    'gaps': imp.load_source('pipe_events_gaps', pp.join(pp.dirname(__file__), 'pipe_events_gaps_dag.py')),
    'encounters': imp.load_source('pipe_events_encounters', pp.join(pp.dirname(__file__), 'pipe_events_encounters_dag.py')),
    'anchorages': imp.load_source('pipe_events_anchorages', pp.join(pp.dirname(__file__), 'pipe_events_anchorages_dag.py')),
    'fishing': imp.load_source('pipe_events_fishing', pp.join(pp.dirname(__file__), 'pipe_events_fishing_dag.py')),
}


class PipelineDagsFactory(DagFactory):
    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(PipelineDagsFactory, self).__init__(pipeline=pipeline, **kwargs)

    def build(self, dag_id):
        dags={}
        for name, dag_factory in subpipelines.iteritems():
            dag_pipeline = '{}.{}'.format(self.pipeline, name)
            newdag_id = '{}.{}'.format(dag_id, name)
            sub_factory = dag_factory.PipelineDagFactory(
                pipeline = dag_pipeline,
                schedule_interval = self.schedule_interval,
                base_config = self.config
            )

            dags[newdag_id] = sub_factory.build(dag_id=newdag_id)

        return dags


events_daily_dags = PipelineDagsFactory().build('pipe_events_daily')
events_monthly_dags = PipelineDagsFactory(
    schedule_interval='@monthly').build('pipe_events_monthly')
events_yearly_dags = PipelineDagsFactory(
    schedule_interval='@yearly').build('pipe_events_yearly')

event_dags = {}
event_dags.update(events_daily_dags)
event_dags.update(events_monthly_dags)
event_dags.update(events_yearly_dags)
for name, dag in event_dags.iteritems():
    globals()[name] = dag
