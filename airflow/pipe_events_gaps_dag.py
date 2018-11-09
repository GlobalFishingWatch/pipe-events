from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from pipe_tools.airflow.models import DagFactory


class PipelineDagFactory(DagFactory):
    def source_date_range(self):
        # shift the date range one day back
        if self.schedule_interval == '@daily':
            return '{{ yesterday_ds }}', '{{ yesterday_ds }}'
        elif self.schedule_interval == '@monthly':
            start_date = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)).strftime("%Y-%m-%d") }}'
            end_date = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-2)).strftime("%Y-%m-%d") }}'
            return start_date, end_date
        else:
            raise ValueError('Unsupported schedule interval {}'.format(
                self.schedule_interval))

    def build(self, dag_id):
        config = self.config
        config['date_range'] = ','.join(self.source_date_range())

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            source_sensors = self.source_table_sensors(dag)

            publish_events = BashOperator(
                task_id='publish_events',
                pool='bigquery',
                bash_command='{docker_run} {docker_image} gap_events '
                             '{date_range} '
                             '{project_id}:{source_dataset}.{source_table} '
                             '{project_id}:{events_dataset}.{events_table} '
                             '{gap_min_pos_count} '
                             '{gap_min_dist} '
                             '{project_id}:{source_dataset}.{segment_vessel}'.format(**config)
            )

            for sensor in source_sensors:
                dag >> sensor >> publish_events

            return dag
