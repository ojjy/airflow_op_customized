import os
import datetime
import pendulum
from airflow import DAG
from airflow.models.variable import Variable
# from slack_operator import task_fail_slack_alert
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.models.baseoperator import chain_linear
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

# command = """
# if [ ! -f /home/ec2-user/data/$(date +"%Y-%m-%d") ]
# then
#     mkdir "/home/ec2-user/data/$(date +"%Y-%m-%d")"
# else
#     echo "File found. Do something meaningful here"
# fi
# """

t1 = BashOperator(
    task_id= 'mkdir_current_date',
    bash_command = 'mkdir -p  "/home/ec2-user/data/$(date +"%Y-%m-%d")"',
    dag= dag
)
prev_task = t1
# filelist = ['RST_BASE_CODE.csv' ,'RST_CHART.csv' ,'RST_CHART_ADDRESS.csv' ,'RST_CHART_SPECIAL.csv' ,'RST_CTI_LOG.csv'
#     ,'RST_CUSTOMER.csv' ,'RST_CUSTOMER_GROUP.csv' ,'RST_CUSTOMER_MEMO.csv' ,'RST_DISEASE.csv' ,'RST_EMPLOYEE.csv'
#     ,'RST_FACTOR.csv' ,'RST_FRONT.csv' ,'RST_FRONT_ADD_INFO.csv' ,'RST_FRONT_CUST_INFO.csv' ,'RST_FRONT_DIAG.csv'
#     ,'RST_FRONT_DISEASE.csv' ,'RST_FRONT_FACTOR.csv' ,'RST_FRONT_NHIS_EDI.csv' ,'RST_FRONT_OPINION.csv' ,'RST_HIRE_OPINION.csv'
#     ,'RST_HOLIDAY.csv' ,'RST_INBODY.csv' ,'RST_ITEM.csv' ,'RST_ITEM_SAPMPLE_ID.csv' ,'RST_MESSAGE.csv' ,'RST_MESSAGE_RECEIVERS.csv'
#     ,'RST_NHIS_EDI.csv' ,'RST_NHIS_OPINION.csv' ,'RST_PACKAGE.csv' ,'RST_PACKAGE_ADD_ITEM.csv'
#     ,'RST_PACKAGE_CHOICE_ITEM.csv' ,'RST_PACKAGE_ITEM.csv' ,'RST_PUSH.csv' ,'RST_QNA.csv' ,'RST_QUESTION.csv'
#     ,'RST_QUESTION_CANCER.csv' ,'RST_QUESTION_STRESS.csv' ,'RST_QUESTION_STRESS1.csv' ,'RST_QUESTION_STRESS2.csv'
#     ,'RST_QUESTION_STRESS3.csv' ,'RST_RESERVE_CAPA.csv' ,'RST_RESERVE_CAPA_ITEM.csv'
#     ,'RST_RESERVE_CHART.csv' ,'RST_RESERVE_CHART_ADDRESS.csv' ,'RST_RESERVE_CHOICE_ITEM.csv' ,'RST_RESERVE_FACTOR.csv'
#     ,'RST_RESERVE_MEMO.csv' ,'RST_RESERVE_MODIFY.csv' ,'RST_RESERVE_PACKAGE.csv' ,'RST_RESERVE_TIME.csv' ,'RST_ESERVE_TIME_DIV.csv' ,'RST_RSERVE_TIME_ID.cv' ,'RST_RESERVE_TIE_TITLE.csv' ,'RST_ROM.csv' ,'RST_ROOMITEM.csv' ,'RST_PC_FACTOR.csv' ,'RST_SP_ITEM.csv'
#     ,'RST_SURVY.csv' ,'RST_SURVEY_ANSWER.cv' ,'RST_SURVEY_JOIN.csv' ,'RST_SURVEY_JOIN_ANSER.csv' ,'RST_SURVEY_QUESTON.csv','RST_RESERVE.csv' ,'RST_TEST_RESULT.csv' ,'RST_TEST_RESULT_TEMP.csv' ,'RST_TREATMENT.csv' ]

filelist = ['RST_SURVEY.csv','RST_RESERVE_TIME_DIV.csv','RST_RESERVE_TIME_TITLE.csv','RST_SURVEY_QUESTION.csv','RST_RESERVE_TIME_ID.csv']

task_list = []
for idx, file in enumerate(filelist):
    logger.info(f"{file} downloading")
    t2 = SFTPToS3Operator(
    task_id=f"sftp_to_s3_{idx}",
    sftp_conn_id="SFTP_CONN_ID_DATAHUB",
    # sftp_path="<full path with filename>",
    sftp_path=f"data_init\\{file}",
    s3_conn_id="S3_CONN_ID_PRIVATE",
    s3_bucket="gcimed-sf",
    s3_key=f"TEST/{file}"
    )
    prev_task >> t2
    prev_task=t2


# t1 >> task_list[0]
# task_list[-1] >> t1
# chain(*task_list)
# task_list

# for i in range(len(task_list)-1):
#     task_list
#     print(task_list[i])
