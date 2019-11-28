from airflow import DAG
from airflow.models import Variable

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory
from airflow_ext.gfw.operators.helper.flexible_operator import FlexibleOperator

PIPELINE = 'pipe_events'
SUBPIPELINE = 'carrier_encounters'


class PipelineDagFactory(DagFactory):
    def __init__(self, interval):
        subpipeline_config_key = '{}.{}'.format(PIPELINE, SUBPIPELINE)
        super(DagFactory, self).__init__(
            pipeline=PIPELINE,
            extra_config=config_tools.load_config(subpipeline_config_key),
            interval=interval
        )

    def build(self, dag_id):
        if self.config.get('enabled', False):
            return

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            self.config['date_range'] = ','.join(self.source_date_range())

            publish_events_bigquery = self.build_docker_task({
                'task_id': 'publish_events_bigquery',
                'pool': 'bigquery',
                'depends_on_past': True,
                'docker_run': self.config['docker_run'],
                'image': self.config['docker_image'],
                'name': 'encounters-publish-events-bigquery',
                'dag': dag,
                'arguments': map(lambda x: x.format(**self.config), [
                    'generate_carrier_encounter_events',
                    ':{source_query}',
                    '{project_id}:{source_dataset}.{vessel_info}',
                    '{project_id}:{events_dataset}.{events_table}'
                ])
            })

            dag >> publish_events_bigquery

            if self.config.get('publish_to_postgres', False):
                publish_events_postgres = self.build_docker_task({
                    'task_id': 'publish_events_postgres',
                    'pool': 'postgres',
                    'docker_run': self.config['docker_run'],
                    'image': self.config['docker_image'],
                    'name': 'encounters-publish-events-postgres',
                    'dag': dag,
                    'arguments': map(lambda x: x.format(**self.config), [
                        'publish_postgres',
                        '{date_range}',
                        '{project_id}:{events_dataset}.{events_table}',
                        '{temp_bucket}',
                        '{postgres_instance}',
                        '{postgres_connection_string}',
                        '{postgres_table}',
                        'encounter'
                    ])
                })
                publish_events_bigquery >> publish_events_postgres

            return dag


for interval in ['daily', 'monthly', 'yearly']:
    dag_id = '{}_{}.{}'.format(PIPELINE, interval, SUBPIPELINE)
    interval = '@{}'.format(interval)
    globals()[dag_id] = PipelineDagFactory(interval).build(dag_id)
