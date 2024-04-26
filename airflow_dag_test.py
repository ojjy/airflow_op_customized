import os
import datetime
import pendulum
from airflow import DAG
from airflow.models.variable import Variable
# from slack_operator import task_fail_slack_alert
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

local_tz = pendulum.timezone('Asia/Seoul')

# dpn_start_date = Variable.get('dpn_start_date')
# dpn_end_date = Variable.get('dpn_end_date')

default_args = {
    'owner': 'yjjo',
    # 'email': 'gcs.kelly@gmail.com',
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'email_on_success': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    # 'on_failure_callback': task_fail_slack_alert,
    'trigger_rule': 'all_done',
}

dag = DAG(
    dag_id=f'{os.path.basename(__file__).replace(".py", "")}',
    default_args = default_args,
    schedule_interval= '@once',
    start_date= datetime.datetime(2024,4,11,1,tzinfo=local_tz),
    catchup= False
)




t1 = BashOperator(
    task_id = 'ssh_op1',
    bash_command='echo $PATH',
    dag=dag

)

t2 = SSHOperator(
    task_id = 'ssh_op3',
    ssh_conn_id='SSH_CONN_ID_CR',
    command='echo "current date: $(date)"&&which python&&python3 /home/ec2-user/test/hello.py',
    cmd_timeout=None,
    dag=dag
)

t1 >> t2