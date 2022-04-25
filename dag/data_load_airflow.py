import os
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


# default arguments for each task
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

DAG_ID = os.path.basename(__file__).replace(".py", "")

          
SPARK_STEPS = [
    {
        'Name': 'Trigger_Source_Target',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', 
                     '--master', 'yarn', 
                     '--jars', '/mnt/MyScalaImport.jar', 
                     '--class', 'abc.def.spark123', 
                     's3://path/sub_path/abc.def.spark-pm_mvp-1.0.0-SNAPSHOT.jar', 
                     'postgresql_cred', 'sql_cred', 'load_type'],
        }
    }
]


JOB_FLOW_OVERRIDES = {
    "Name": "emr-cluster-airflow",
    "LogUri": "s3://logs/elasticmapreduce/",
    "ReleaseLabel": "emr-6.4.0",
    "Applications": [
        {"Name": "Spark"},
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        'Ec2KeyName': 'PEM_file_name', 
        "Ec2SubnetId": "subnet-id"
    },
    'BootstrapActions': [
        {
            'Name': 'import custom Jars',
            'ScriptBootstrapAction': {
                'Path': 's3://path/sub_path/abc/copytoolsjar.sh',
                'Args': ['s3://path/sub_path/postgresql-42.2.24.jar',
                            's3://path/sub_path/mssql-jdbc-8.4.1.jre8.jar']
            }
        }
    ],
    'Configurations': [
      {
        'Classification': 'spark-defaults',
            "Properties": {
            "spark.jars": "/usr/lib/spark/jars/mssql-jdbc-8.4.1.jre8.jar,/usr/lib/spark/jars/postgresql-42.2.24.jar"
        }
      }
    ],
    "VisibleToAllUsers": True,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "Tags": [
        {"Key": "Environment", "Value": "Development"},
    ],
}

with DAG(
    dag_id=DAG_ID,
    description="Run built-in Spark app on Amazon EMR",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["emr", "spark"],
) as dag:
    
    Start =  DummyOperator(task_id = 'Start')
	
    cluster_creator = EmrCreateJobFlowOperator(
        task_id = "create_job_flow", 
        aws_conn_id = "aws_default",
        emr_conn_id = "emr_default",
        job_flow_overrides = JOB_FLOW_OVERRIDES
    )
    
    Trigger_Source_Target_PHI_non_PHI = EmrAddStepsOperator(
        task_id = "add_steps",
        job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id = "aws_default",
        steps=SPARK_STEPS,
    )
    
    step_checker = EmrStepSensor(
        task_id = "watch_step",
        job_flow_id = "{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id = "{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id = "aws_default",
    )
    
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
    )
    
    End = DummyOperator(task_id = 'End')
    
    Start >> cluster_creator >> Trigger_Source_Target_PHI_non_PHI >> step_checker >> cluster_remover >> End
