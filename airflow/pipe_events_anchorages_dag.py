from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory
from airflow_ext.gfw.operators.helper.flexible_operator import FlexibleOperator

import sys
import os
# https://stackoverflow.com/questions/50150384/importing-local-module-python-script-in-airflow-dag
# can not import under dag_folder from an easy way
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from pipe_events_dag import PipelineEventsDagFactory


class PipelineDagFactory(PipelineEventsDagFactory):
    def __init__(self, anchorages_config, **kwargs):
        super(PipelineDagFactory, self).__init__(**kwargs)
        self.anchorages_config = anchorages_config

    def build(self, dag_id):
        config = self.config.copy()
        config.update(self.anchorages_config)
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
                'name':'anchorages-publish-events-bigquery',
                'dag':dag,
                'cmds':['generate_anchorage_events',
                        '{date_range}'.format(**config),
                        '{project_id}:{source_dataset}.{source_table}'.format(**config),
                        '{project_id}:{source_dataset}.{vessel_info}'.format(**config),
                        '{project_id}:{anchorages_dataset}.{named_anchorages}'.format(**config),
                        '{project_id}:{events_dataset}.{events_table}'.format(**config)]
            }
            publish_events_bigquery = FlexibleOperator(publish_events_bigquery_params).build_operator('{flexible_operator}'.format(**config))

            publish_events_postgres_params = {
                'task_id':'publish_events_postgres',
                'pool':'postgres',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'anchorages-publish-events-postgres',
                'dag':dag,
                'cmds':['publish_postgres',
                        '{date_range}'.format(**config),
                        '{project_id}:{events_dataset}.{events_table}'.format(**config),
                        '{temp_bucket}'.format(**config),
                        '{postgres_instance}'.format(**config),
                        '{postgres_connection_string}'.format(**config),
                        '{postgres_table}'.format(**config),
                        'port']
            }
            publish_events_postgres = FlexibleOperator(publish_events_postgres_params).build_operator('{flexible_operator}'.format(**config))

            for sensor in source_sensors:
                dag >> sensor >> publish_events_bigquery >> publish_events_postgres

            return dag

anchorages_config = config_tools.load_config('pipe_events.anchorages')
events_anchorages_daily_dag = PipelineDagFactory(anchorages_config).build('pipe_events_daily.anchorages')
events_anchorages_monthly_dag = PipelineDagFactory(anchorages_config, schedule_interval='@monthly').build('pipe_events_monthly.anchorages')
events_anchorages_yearly_dag = PipelineDagFactory(anchorages_config, schedule_interval='@yearly').build('pipe_events_yearly.anchorages')
