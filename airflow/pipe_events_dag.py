from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

from pipe_tools.airflow.models import DagFactory


PIPELINE = "pipe_events"


class TemplatedBigQueryCheckOperator(BigQueryCheckOperator):
    template_fields = ('sql',)


class PipeEventsDagFactory(DagFactory):
    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(PipeEventsDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def build(self, dag_id):
        config = self.config
        # config['date_range'] = ','.join(self.source_date_range())
        #
        # with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
        #     source_sensors = self.source_table_sensors(dag)
        #
        #     segment_identity_exists = TemplatedBigQueryCheckOperator(
        #         task_id='segment_identity_exists',
        #         sql='SELECT count(*) FROM '
        #             '[{project_id}:{pipeline_dataset}.{segment_identity}{first_day_of_month_nodash}] '
        #             'WHERE last_timestamp > TIMESTAMP("{ds} 01:00")'.format(**config),
        #         retries=24 * 7,  # retry once per hour for a week
        #         retry_delay=timedelta(minutes=60)
        #     )
        #
        #     add_measures = BashOperator(
        #         task_id='add_measures',
        #         pool='bigquery',
        #         bash_command='{docker_run} {docker_image} add_measures '
        #                      '{date_range} '
        #                      '{project_id}.{source_dataset}.{source_table} '
        #                      '{project_id}:{pipeline_dataset}.{dest_table} '
        #                      '{project_id}.{pipeline_dataset}.{segments} '
        #                      '{project_id}.{pipeline_dataset}.{segment_identity}{first_day_of_month_nodash} '
        #                      '{project_id}.{pipeline_dataset}.{measures} '
        #                      ''.format(**config)
        #     )
        #
        #     for sensor in source_sensors:
        #         dag >> sensor >> add_measures
        #     dag >> segment_identity_exists >> add_measures
        #
        # return dag


# pipe_measures_daily_dag = PipeMeasuresDagFactory().build('pipe_measures_daily')
# pipe_measures_monthly_dag = PipeMeasuresDagFactory(schedule_interval='@monthly').build('pipe_measures_monthly')
