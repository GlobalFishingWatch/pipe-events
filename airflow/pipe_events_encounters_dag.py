from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from pipe_tools.airflow.models import DagFactory


class PipelineDagFactory(DagFactory):
    def build(self, dag_id):
        config = self.config
        config['date_range'] = ','.join(self.source_date_range())

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            publish_events = BashOperator(
                task_id='publish_events',
                pool='bigquery',
                bash_command='{docker_run} {docker_image} encounter_events '
                             '{project_id}:{source_dataset}.{source_table} '
                             '{project_id}:{events_dataset}.{events_table}'.format(
                                 **config)
            )

            dag >> publish_events

            return dag
