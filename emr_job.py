from datetime import timedelta
from textwrap import dedent
import boto3

# The DAG object; we'll need this to instantiate a DAG
import airflow
from airflow import DAG

# Operators; we need this to operate!
#from airflow.operators.bash import BashOperator
#from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

#from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
#from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

# helper functions
def retrieve_s3_file(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location']
    file_ext = path.splitext(s3_location)[-1].lstrip('.').capitalize()
    # file_ext = 
    print(f"Data engineering pipeline logging...S3 location: {s3_location}")
    kwargs['ti'].xcom_push(key='s3_location', value=s3_location)
    kwargs['ti'].xcom_push(key='file_ext', value=file_ext)

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['NHATT416@GMAIL.COM'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': airflow.utils.dates.days_ago(0),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

EMR_CLUSTER_ID = 'j-3MM8L3MY7RQRQ'
INPUT_DATA_CSV = 's3://wcd-data-engineering-pipeline/source/banking.csv'
OUTPUT_DIRECTORY = 's3://wcd-data-engineering-pipeline/output'
ARTIFACT_JAR = 's3://wcd-data-engineering-pipeline/spark-engine_2.11-0.0.1.jar'


# TODO think of solution (and function) to dynamically calculate amount of resources
# (e.g. num-executors, executor-memory, executor-cores, etc.) to allocate to
# the given job 
SPARK_STEPS = [
    {
        'Name': 'wcd_data_processing_engine',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                '--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                '--num-executors', '2',
                '--executor-memory', '3g',
                '--driver-memory', '1g',
                '--executor-cores', '1',
                ARTIFACT_JAR, # artifact
                '-p', 'wcd-demo',
               
                '-i', "{{ task_instance.xcom_pull(task_ids='parse_request', key='file_ext') }}",  # should have a function to dynamically set this parameter based on file extension
                '-o', 'parquet',
                # TODO: change this to parse s3 location with incoming event that triggers Lambda function 
                #'-s', INPUT_DATA_CSV,
                '-s', "{{ task_instance.xcom_pull(task_ids='parse_request', key='s3_location') }}",
                '-d', OUTPUT_DIRECTORY,
                '-c', 'state', # partition column
                '-m', 'append',
                '--input-options', 'header=true'
                ]
        }
    }
]

with DAG(
    'data_pipeline_emr',
    default_args=default_args,
    description='A data pipline EMR DAG',
    dagrun_timeout=timedelta(hours=2),
    tags=['emr'],
) as dag:

    parse_request = PythonOperator(
        task_id='parse_request',
        provide_context=True,
        python_callable=retrieve_s3_file
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id=EMR_CLUSTER_ID,
        aws_conn_id='aws_default',
        steps=SPARK_STEPS
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    parse_request >> step_adder >> step_checker
