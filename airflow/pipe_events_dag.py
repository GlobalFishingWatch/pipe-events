import posixpath as pp
import imp

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from pipe_tools.airflow.models import DagFactory

PIPELINE = "pipe_events"

subpipelines = {
    'gaps': imp.load_source('pipe_events_gaps', pp.join(pp.dirname(__file__), 'pipe_events_gaps_dag.py')),
    'encounters': imp.load_source('pipe_events_encounters', pp.join(pp.dirname(__file__), 'pipe_events_encounters_dag.py')),
    'anchorages': imp.load_source('pipe_events_anchorages', pp.join(pp.dirname(__file__), 'pipe_events_anchorages_dag.py')),
    'fishing': imp.load_source('pipe_events_fishing', pp.join(pp.dirname(__file__), 'pipe_events_fishing_dag.py')),
}


class PipelineDagFactory(DagFactory):
    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(PipelineDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def build(self, dag_id):
        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            for subdag_name, dag_factory in subpipelines.iteritems():
                subdag_pipeline = '{}.{}'.format(self.pipeline, subdag_name)
                subdag_id = '{}.{}'.format(dag_id, subdag_name)
                subdag_factory = dag_factory.PipelineDagFactory(
                    pipeline=subdag_pipeline, schedule_interval=self.schedule_interval, base_config=self.config)

                subdag = SubDagOperator(
                    subdag=subdag_factory.build(dag_id=subdag_id),
                    task_id=subdag_name,
                    depends_on_past=True,
                    dag=dag
                )
            return dag


events_daily_dag = PipelineDagFactory().build('pipe_events_daily')
events_monthly_dag = PipelineDagFactory(
    schedule_interval='@monthly').build('pipe_events_monthly')
