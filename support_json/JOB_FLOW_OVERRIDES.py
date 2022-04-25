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
