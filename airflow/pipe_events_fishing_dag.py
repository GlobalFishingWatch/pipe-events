from airflow import DAG
from airflow.models import Variable

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory
from airflow_ext.gfw.operators.helper.flexible_operator import FlexibleOperator

from datetime import datetime, timedelta

PIPELINE = 'pipe_events'
SUBPIPELINE = 'fishing'


class PipelineDagFactory(DagFactory):
    def __init__(self, interval):
        subpipeline_config_key = '{}.{}'.format(PIPELINE, SUBPIPELINE)
        super(DagFactory, self).__init__(
            pipeline=PIPELINE,
            extra_config=config_tools.load_config(subpipeline_config_key),
            interval=interval
        )

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
        elif self.schedule_interval == '@yearly':
            start_date_template = '{{{{ (execution_date.replace(day=1) + {buffer_delta_expression}).strftime("%Y-%m-%d") }}}}'
            start_date = start_date_template.format(**expressions)
            end_date_template = '{{{{ (execution_date.replace(day=1, month=1) + macros.dateutil.relativedelta.relativedelta(years=1)).strftime("%Y-%m-%d") }}}}'
            end_date = end_date_template.format(**expressions)
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
                'name': 'fishing-publish-events-bigquery',
                'dag': dag,
                'arguments': map(lambda x: x.format(**self.config), [
                    'generate_fishing_events',
                    '{date_range}',
                    '{project_id}:{source_dataset}.{source_table}',
                    '{source_filter}',
                    '{project_id}:{source_dataset}.{segment_vessel}',
                    '{project_id}:{source_dataset}.{segment_info}',
                    '{project_id}:{source_dataset}.{vessel_info}',
                    '{project_id}:{events_dataset}.{events_table}',
                    '{min_event_duration}'
                ])
            })

            for sensor in source_sensors:
                dag >> sensor >> publish_events_bigquery

            if self.config.get('publish_to_postgres', False):
                publish_events_postgres = self.build_docker_image({
                    'task_id': 'publish_events_postgres',
                    'pool': 'postgres',
                    'docker_run': self.config['docker_run'],
                    'image': self.config['docker_image'],
                    'name': 'fishing-publish-events-postgres',
                    'dag': dag,
                    'arguments': map(lambda x: x.format(**self.config), [
                        'publish_postgres',
                        '{date_range}',
                        '{project_id}:{events_dataset}.{events_table}',
                        '{temp_bucket}',
                        '{postgres_instance}',
                        '{postgres_connection_string}',
                        '{postgres_table}',
                        'fishing'
                    ])
                })
                publish_events_bigquery >> publish_events_postgres

            return dag


for interval in ['daily', 'monthly', 'yearly']:
    dag_id = '{}_{}.{}'.format(PIPELINE, interval, SUBPIPELINE)
    interval = '@{}'.format(interval)
    globals()[dag_id] = PipelineDagFactory(interval).build(dag_id)
