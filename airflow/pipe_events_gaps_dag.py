from airflow import DAG
from airflow.models import Variable

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory
from airflow_ext.gfw.operators.helper.flexible_operator import FlexibleOperator

from datetime import datetime, timedelta

PIPELINE = 'pipe_events'
SUBPIPELINE = 'gaps'


class PipelineDagFactory(DagFactory):
    def __init__(self, interval):
        subpipeline_config_key = '{}.{}'.format(PIPELINE, SUBPIPELINE)
        super(DagFactory, self).__init__(
            pipeline=PIPELINE,
            extra_config=config_tools.load_config(subpipeline_config_key),
            interval=interval
        )

    def source_date_range(self):
        # shift the date range one day back
        if self.schedule_interval == '@daily':
            return '{{ yesterday_ds }}', '{{ yesterday_ds }}'
        elif self.schedule_interval == '@monthly':
            start_date = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)).strftime("%Y-%m-%d") }}'
            end_date = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-2)).strftime("%Y-%m-%d") }}'
            return start_date, end_date
        elif self.schedule_interval == '@yearly':
            start_date = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)).strftime("%Y-%m-%d") }}'
            end_date = '{{ (execution_date.replace(day=1, month=1) + macros.dateutil.relativedelta.relativedelta(years=1, days=-2)).strftime("%Y-%m-%d") }}'
            return start_date, end_date
        else:
            raise ValueError('Unsupported schedule interval {}'.format(
                self.schedule_interval))

    def build(self, dag_id):
        if self.config.get('enabled', False):
            return

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            self.config['date_range'] = ','.join(self.source_date_range())
            source_sensors = self.source_table_sensors(dag)

            publish_events_bigquery = self.build_docker_task({
                'task_id': 'publish_events_bigquery',
                'pool': 'bigquery',
                'depends_on_past': True,
                'docker_run': self.config['docker_run'],
                'image': self.config['docker_image'],
                'name': 'gaps-publish-events-bigquery',
                'dag': dag,
                'arguments': map(lambda x: x.format(**self.config), [
                    'generate_gap_events',
                    '{date_range}',
                    '{project_id}:{source_dataset}.{source_table}',
                    '{project_id}:{events_dataset}.{events_table}',
                    '{project_id}:{source_dataset}.{segment_vessel}',
                    '{project_id}:{source_dataset}.{vessel_info}',
                    '{gap_min_pos_count}',
                    '{gap_min_dist}'
                ])
            })

            for sensor in source_sensors:
                dag >> sensor >> publish_events_bigquery

            if self.config.get('publish_to_postgres', False):
                publish_events_postgres = {
                    'task_id': 'publish_events_postgres',
                    'pool': 'postgres',
                    'docker_run': self.config['docker_run'],
                    'image': self.config['docker_image'],
                    'name': 'gaps-publish-events-postgres',
                    'dag': dag,
                    'arguments': map(lambda x: x.format(**self.config), [
                        'publish_postgres',
                        '{date_range}',
                        '{project_id}:{events_dataset}.{events_table}',
                        '{temp_bucket}',
                        '{postgres_instance}',
                        '{postgres_connection_string}',
                        '{postgres_table}',
                        'gap'])
                }
                publish_events_bigquery >> publish_events_postgres

            return dag


for interval in ['daily', 'monthly', 'yearly']:
    dag_id = '{}_{}.{}'.format(PIPELINE, interval, SUBPIPELINE)
    interval = '@{}'.format(interval)
    globals()[dag_id] = PipelineDagFactory(interval).build(dag_id)
