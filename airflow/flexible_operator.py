from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator

import os


class FlexibleOperator():
    def __init__(self, parameters):
        self.operator_parameters = parameters

    def build_operator(self, kind):
        if kind != 'bash' or kind != 'kubernetes':
            raise ValueError('The operators allowed are bash or kubernetes, not <{}>'.format(kind))
        operator = None
        task_id = self.operator_parameters['task_id']
        pool = self.operator_parameters['pool']
        cmds = self.operator_parameters['cmds']
        if kind == 'bash':
            commands = '{} {} {}'.format(
                self.operator_parameters['docker_run'],
                self.operator_parameters['image'],
                ' '.join(cmds))
            operator = BashOperator(
                task_id = task_id,
                pool = pool,
                bash_command = commands
            )
        else:
            commands = ['./scripts/run.sh']
            commands.append(cmds)
            operator = KubernetesPodOperator(
                image = self.operator_parameters['image'],
                name = self.operator_parameters['name'],
                dag = self.operator_parameters['dag'],
                cmds = commands,
                namespace = os.getenv('K8_NAMESPACE'),
                task_id = task_id,
                pool = pool,
                get_logs = True,
                in_cluster = True
            )
        return operator

