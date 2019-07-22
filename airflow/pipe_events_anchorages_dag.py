from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow_ext.gfw.models import DagFactory
import FlexibleOperator


class PipelineDagFactory(DagFactory):
    def build(self, dag_id):
        config = self.config
        config['date_range'] = ','.join(self.source_date_range())

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            source_sensors = self.source_table_sensors(dag)

            publish_events_bigquery_param = {
                'docker_run':'{docker_run}'.format(**config),
                'task_id':'publish_events_bigquery',
                'pool':'bigquery',
                'name':'publish_events_bigquery',
                'dag':dag,
                'image':'{docker_image}'.format(**config),
                'cmds':['generate_anchorage_events',
                        '{date_range}'.format(**config),
                        '{project_id}:{source_dataset}.{source_table}'.format(**config),
                        '{project_id}:{source_dataset}.{vessel_info}'.format(**config),
                        '{project_id}:{anchorages_dataset}.{named_anchorages}'.format(**config),
                        '{project_id}:{events_dataset}.{events_table}'.format(**config)]
            }
            publish_events_bigquery = FlexibleOperator(publish_events_bigquery_param).build_operator('bash')


            publish_events_postgres_param = {
                'docker_run':'{docker_run}'.format(**config),
                'task_id':'publish_events_postgres',
                'pool':'postgres',
                'name':'publish_events_postgres',
                'dag':dag,
                'image':'{docker_image}'.format(**config),
                'cmds':['publish_postgres',
                        '{date_range}'.format(**config),
                        '{project_id}:{events_dataset}.{events_table}'.format(**config),
                        '{temp_bucket}'.format(**config),
                        '{postgres_instance}'.format(**config),
                        '{postgres_connection_string}'.format(**config),
                        '{postgres_table}'.format(**config),
                        'port']
            }
            publish_events_postgres = FlexibleOperator(publish_events_postgres_param).build_operator('bash')

            for sensor in source_sensors:
                dag >> sensor >> publish_events_bigquery >> publish_events_postgres

            return dag
