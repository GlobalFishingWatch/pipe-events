from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from pipe_tools.airflow.models import DagFactory


class PipelineDagFactory(DagFactory):
    def source_date_range(self):
        # The scored messages only have logistic scores for a couple of days
        # while we accumulate the amount of data we need to refine that score
        # with the nnet model. We need to shift the intervals' start date so
        # that it reprocesses the events for up to that amount days before the
        # current day.
        inference_buffer_days = Variable.get("INFERENCE_BUFFER_DAYS", 7)
        expressions = {
            'buffer_delta_expression': 'macros.dateutil.relativedelta.relativedelta(days=-{})'.format(inference_buffer_days),
        }

        if self.schedule_interval == '@daily':
            start_date_template = '{{{{ (execution_date + {buffer_delta_expression}).strftime("%Y-%m-%d") }}}}'
            start_date = start_date_template.format(**expressions)
            end_date_template = '{{{{ ds }}}}'
            end_date = end_date_template.format(**expressions)
            return start_date, end_date
        elif self.schedule_interval == '@monthly':
            start_date_template = '{{{{ (execution_date.replace(day=1) + {buffer_delta_expression}).strftime("%Y-%m-%d") }}}}'
            start_date = start_date_template.format(**expressions)
            end_date_template = '{{{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1)).strftime("%Y-%m-%d") }}}}'
            end_date = end_date_template.format(**expressions)
            return start_date, end_date
        else:
            raise ValueError('Unsupported schedule interval {}'.format(
                self.schedule_interval))

    def build(self, dag_id):
        config = self.config
        config['date_range'] = ','.join(self.source_date_range())

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            source_sensors = self.source_table_sensors(dag)

            publish_events_bigquery = BashOperator(
                task_id='publish_events_bigquery',
                pool='bigquery',
                bash_command='{docker_run} {docker_image} generate_fishing_events '
                '{date_range} '
                '{project_id}:{source_dataset}.{source_table} '
                '{project_id}:{source_dataset}.{segment_vessel} '
                '{project_id}:{source_dataset}.{segment_info} '
                '{project_id}:{source_dataset}.{vessel_info} '
                '{project_id}:{events_dataset}.{events_table} '
                '{min_event_duration}'.format(**config)
            )

            publish_events_postgres = BashOperator(
                task_id='publish_events_postgres',
                pool='postgres',
                bash_command='{docker_run} {docker_image} publish_postgres '
                '{date_range} '
                '{project_id}:{events_dataset}.{events_table} '
                '{temp_bucket} '
                '{postgres_connection_string} '
                '{postgres_table} '
                'fishing'.format(**config)
            )

            for sensor in source_sensors:
                dag >> sensor >> publish_events_bigquery >> publish_events_postgres

            return dag
