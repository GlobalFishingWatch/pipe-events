from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

from pipe_tools.airflow.models import DagFactory


PIPELINE = "pipe_events"


class TemplatedBigQueryCheckOperator(BigQueryCheckOperator):
    template_fields = ('sql',)


class GapEventsDagFactory(DagFactory):
    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(GapEventsDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def source_date_range(self):
        if self.schedule_interval == '@daily':
            return '{{ yesterday_ds }}', '{{ ds }}'
        elif self.schedule_interval == '@monthly':
            start_date = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)).strftime("%Y-%m-%d") }}'
            end_date = self.config['last_day_of_month']
            return start_date, end_date
        else:
            raise ValueError('Unsupported schedule interval {}'.format(self.schedule_interval))

    def build(self, dag_id):
        config = self.config
        config['source_table'] = config['position_messages']
        config['date_range'] = ','.join(self.source_date_range())

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            source_sensors = self.source_table_sensors(dag)

            publish_events = BashOperator(
                task_id='publish_events',
                bash_command='{docker_run} {docker_image} gap_events '
                             '{date_range} '
                             '{project_id}:{source_dataset}.{position_messages} '
                             '{project_id}:{events_dataset}.{gap_events_table}'.format(**config)
            )

            for sensor in source_sensors:
                dag >> sensor >> publish_events

            return dag


gaps_events_daily_dag = GapEventsDagFactory().build('gap_events_daily')
gaps_events_monthly_dag = GapEventsDagFactory(schedule_interval='@monthly').build('gap_events_monthly')
