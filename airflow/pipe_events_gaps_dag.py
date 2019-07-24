from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory

from datetime import datetime, timedelta

import sys
import os
# https://stackoverflow.com/questions/50150384/importing-local-module-python-script-in-airflow-dag
# can not import under dag_folder from an easy way
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from pipe_events_dag import PipelineEventsDagFactory


class PipelineDagFactory(PipelineEventsDagFactory):
    def __init__(self, gaps_config, **kwargs):
        super(PipelineDagFactory, self).__init__(**kwargs)
        self.gaps_config = gaps_config

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
        config = self.config.copy()
        config.update(self.gaps_config)
        config['date_range'] = ','.join(self.source_date_range())

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            source_sensors = self.source_table_sensors(dag)

            publish_events_bigquery = BashOperator(
                task_id='publish_events_bigquery',
                pool='bigquery',
                depends_on_past=True,
                bash_command='{docker_run} {docker_image} generate_gap_events '
                             '{date_range} '
                             '{project_id}:{source_dataset}.{source_table} '
                             '{project_id}:{events_dataset}.{events_table} '
                             '{project_id}:{source_dataset}.{segment_vessel} '
                             '{project_id}:{source_dataset}.{vessel_info} '
                             '{gap_min_pos_count} '
                             '{gap_min_dist} '.format(**config)
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
                'gap'.format(**config)
            )

            for sensor in source_sensors:
                dag >> sensor >> publish_events_bigquery >> publish_events_postgres

            return dag

gaps_config = config_tools.load_config('pipe_events.gaps')
events_gaps_daily_dag = PipelineDagFactory(gaps_config).build('pipe_events_daily.gaps')
events_gaps_monthly_dag = PipelineDagFactory(gaps_config, schedule_interval='@monthly').build('pipe_events_monthly.gaps')
events_gaps_yearly_dag = PipelineDagFactory(gaps_config, schedule_interval='@yearly').build('pipe_events_yearly.gaps')
