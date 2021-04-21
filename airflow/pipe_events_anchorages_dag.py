from airflow import DAG

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory

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

        range=self.source_date_range()
        config['date_range'] = range[0] + ',' + range[1]
        config['start_date'] = range[0]
        config['end_date'] = range[1]

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            self.config = config
            source_sensors = self.source_table_sensors(dag)
            publish_to_postgres = config.get('publish_to_postgres',False)

            publish_events_bigquery = self.build_docker_task({
                'task_id':'publish_events_bigquery',
                'pool':'bigquery',
                'depends_on_past':True,
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'anchorages-publish-events-bigquery',
                'dag':dag,
                'arguments':['generate_anchorage_events',
                             '{date_range}'.format(**config),
                             '{project_id}:{source_dataset}.{source_table}'.format(**config),
                             '{project_id}:{source_dataset}.{vessel_info}'.format(**config),
                             '{project_id}:{anchorages_dataset}.{named_anchorages}'.format(**config),
                             '{project_id}:{events_dataset}.{events_table}'.format(**config)]
            })


            for sensor in source_sensors:
                dag >> sensor >> publish_events_bigquery

            if publish_to_postgres:
                publish_events_postgres = self.build_docker_task({
                    'task_id':'publish_events_postgres',
                    'pool':'postgres',
                    'docker_run':'{docker_run}'.format(**config),
                    'image':'{docker_image}'.format(**config),
                    'name':'anchorages-publish-events-postgres',
                    'dag':dag,
                    'arguments':['publish_postgres',
                                 '{start_date}'.format(**config),
                                 '{end_date}'.format(**config),
                                 '{project_id}:{events_dataset}.{events_table}'.format(**config),
                                 '{temp_bucket}'.format(**config),
                                 '{project_id}'.format(**config),
                                 '{postgres_database_region}'.format(**config),
                                 '{postgres_db_instance_name}'.format(**config),
                                 '{postgres_database}'.format(**config),
                                 '{postgres_db_user}'.format(**config),
                                 '{postgres_db_password}'.format(**config),
                                 '{postgres_db_table}'.format(**config),
                                 'port']
                })
                publish_events_bigquery >> publish_events_postgres

            return dag

anchorages_config = config_tools.load_config('pipe_events.anchorages')
events_anchorages_daily_dag = PipelineDagFactory(anchorages_config).build('pipe_events_daily.anchorages')
events_anchorages_monthly_dag = PipelineDagFactory(anchorages_config, schedule_interval='@monthly').build('pipe_events_monthly.anchorages')
events_anchorages_yearly_dag = PipelineDagFactory(anchorages_config, schedule_interval='@yearly').build('pipe_events_yearly.anchorages')
