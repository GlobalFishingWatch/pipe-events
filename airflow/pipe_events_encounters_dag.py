from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow_ext.gfw.models import DagFactory


class PipelineDagFactory(DagFactory):
    def build(self, dag_id):
        config = self.config
        config['date_range'] = ','.join(self.source_date_range())

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            publish_events_bigquery = BashOperator(
                task_id='publish_events_bigquery',
                pool='bigquery',
                bash_command='{docker_run} {docker_image} generate_encounter_events '
                             '{project_id}:{source_dataset}.{source_table} '
                             '{project_id}:{events_dataset}.{events_table}'.format(
                                 **config)
            )

            publish_events_postgres = BashOperator(
                task_id='publish_events_postgres',
                bash_command='{docker_run} {docker_image} publish_postgres '
                '{date_range} '
                '{project_id}:{events_dataset}.{events_table} '
                '{temp_bucket} '
                '{postgres_instance} '
                '{postgres_connection_string} '
                '{postgres_table} '
                'encounter'.format(**config)
            )

            dag >> publish_events_bigquery >> publish_events_postgres

            return dag
