import os
import datetime
import pendulum
from airflow import DAG
from airflow.models.variable import Variable
# from slack_operator import task_fail_slack_alert
from airflow.operators.bash import BashOperator
from sftptos3_multiple_files_download_operator import SFTPToS3MultipleFilesDownloadOperator
import logging


logger = logging.getLogger(__name__)
local_tz = pendulum.timezone('Asia/Seoul')

# dpn_start_date = Variable.get('dpn_start_date')
# dpn_end_date = Variable.get('dpn_end_date')

default_args = {
    'owner': 'yjjo',
    # 'email': 'gcs.kelly@gmail.com',
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'email_on_success': True,
    'retries': 3,
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
    task_id= 'mkdir_current_date',
    bash_command = 'mkdir -p  "/home/ec2-user/data/$(date +"%Y-%m-%d")"',
    dag= dag
)

t2 = SFTPToS3MultipleFilesDownloadOperator(
    task_id='sftp_to_s3_multiple_files',
    ssh_conn_id='SFTP_CONN_ID_DATAHUB',
    filetype='csv',
    remote_filelist=['RST_QNA.csv','RST_CUSTOMER_GROUP.csv', 'RST_CUSTOMER_MEMO.csv',
            'RST_CHART.csv', 'RST_CTI_LOG.csv', 'RST_FRONT_DISEASE.csv',
            'RST_HIRE_OPINION.csv','RST_FRONT_OPINION.csv','RST_NHIS_OPINION.csv'],
    remote_filepath=f'{os.path.join(".", "data_init")}',
    s3_conn_id = "S3_CONN_ID_PRIVATE",
    s3_bucket = "gcimed-sf",
    s3_key = f"TEST"

)
t1 >> t2