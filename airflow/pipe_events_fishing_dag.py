from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory
from airflow_ext.gfw.operators.helper.flexible_operator import FlexibleOperator

from datetime import datetime, timedelta

import sys
import os
# https://stackoverflow.com/questions/50150384/importing-local-module-python-script-in-airflow-dag
# can not import under dag_folder from an easy way
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from pipe_events_dag import PipelineEventsDagFactory


class PipelineDagFactory(PipelineEventsDagFactory):
    def __init__(self, fishing_config, **kwargs):
        super(PipelineDagFactory, self).__init__(**kwargs)
        self.fishing_config = fishing_config

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
        config = self.config.copy()
        config.update(self.fishing_config)
        config['date_range'] = ','.join(self.source_date_range())

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            self.config = config
            source_sensors = self.source_table_sensors(dag)

            publish_events_bigquery_params = {
                'task_id':'publish_events_bigquery',
                'pool':'bigquery',
                'depends_on_past':True,
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'fishing-publish-events-bigquery',
                'dag':dag,
                'cmds':['generate_fishing_events',
                        '{date_range}'.format(**config),
                        '{project_id}:{source_dataset}.{source_table}'.format(**config),
                        '{project_id}:{source_dataset}.{segment_vessel}'.format(**config),
                        '{project_id}:{source_dataset}.{segment_info}'.format(**config),
                        '{project_id}:{source_dataset}.{vessel_info}'.format(**config),
                        '{project_id}:{events_dataset}.{events_table}'.format(**config),
                        '{min_event_duration}'.format(**config)]
            }
            publish_events_bigquery = FlexibleOperator(publish_events_bigquery_params).build_operator('{flexible_operator}'.format(**config))

            publish_events_postgres_params = {
                'task_id':'publish_events_postgres',
                'pool':'postgres',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'fishing-publish-events-postgres',
                'dag':dag,
                'cmds':['publish_postgres',
                        '{date_range}'.format(**config),
                        '{project_id}:{events_dataset}.{events_table}'.format(**config),
                        '{temp_bucket}'.format(**config),
                        '{postgres_instance}'.format(**config),
                        '{postgres_connection_string}'.format(**config),
                        '{postgres_table}'.format(**config),
                        'fishing']
            }
            publish_events_postgres = FlexibleOperator(publish_events_postgres_params).build_operator('{flexible_operator}'.format(**config))

            for sensor in source_sensors:
                dag >> sensor >> publish_events_bigquery >> publish_events_postgres

            return dag

fishing_config = config_tools.load_config('pipe_events.fishing')
events_fishing_daily_dag = PipelineDagFactory(fishing_config).build('pipe_events_daily.fishing')
events_fishing_monthly_dag = PipelineDagFactory(fishing_config, schedule_interval='@monthly').build('pipe_events_monthly.fishing')
events_fishing_yearly_dag = PipelineDagFactory(fishing_config, schedule_interval='@yearly').build('pipe_events_yearly.fishing')
