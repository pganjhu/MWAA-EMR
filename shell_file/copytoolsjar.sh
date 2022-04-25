#!/bin/bash
aws s3 cp s3://path/sub_path/abc.def.spark-mvp-1.0.0-SNAPSHOT.jar /mnt/MyScalaImport.jar && bash -c "sudo aws s3 cp s3://path/sub_path/mssql-jdbc-8.4.1.jre8.jar /usr/lib/spark/jars/" && bash -c "sudo aws s3 cp s3://path/sub_path/postgresql-42.2.24.jar /usr/lib/spark/jars/"
