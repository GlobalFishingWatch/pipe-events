from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory

import sys
import os
# https://stackoverflow.com/questions/50150384/importing-local-module-python-script-in-airflow-dag
# can not import under dag_folder from an easy way
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from pipe_events_dag import PipelineEventsDagFactory


class PipelineDagFactory(PipelineEventsDagFactory):
    def __init__(self, encounters_config, **kwargs):
        super(PipelineDagFactory, self).__init__(**kwargs)
        self.encounters_config = encounters_config

    def build(self, dag_id):
        config = self.config.copy()
        config.update(self.encounters_config)
        config['date_range'] = ','.join(self.source_date_range())

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            publish_events_bigquery = BashOperator(
                task_id='publish_events_bigquery',
                pool='bigquery',
                depends_on_past=True,
                bash_command='{docker_run} {docker_image} generate_encounter_events '
                             '{project_id}:{source_dataset}.{source_table} '
                             '{project_id}:{source_dataset}.{vessel_info} '
                             '{project_id}:{events_dataset}.{events_table}'.format(
                                 **config)
            )

            publish_events_postgres = BashOperator(
                task_id='publish_events_postgres',
                pool='postgres',
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

encounters_config = config_tools.load_config('pipe_events.encounters')
events_encounters_daily_dag = PipelineDagFactory(encounters_config).build('pipe_events_daily.encounters')
events_encounters_monthly_dag = PipelineDagFactory(encounters_config, schedule_interval='@monthly').build('pipe_events_monthly.encounters')
events_encounters_yearly_dag = PipelineDagFactory(encounters_config, schedule_interval='@yearly').build('pipe_events_yearly.encounters')
