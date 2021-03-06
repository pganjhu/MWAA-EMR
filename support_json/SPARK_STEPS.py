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
