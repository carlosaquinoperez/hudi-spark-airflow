from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 13),
    "retries": 1,
}

dag = DAG(
    "hudi_spark_pipeline",
    default_args=default_args,
    description="Pipeline para procesamiento con Apache Hudi en Spark",
    schedule_interval=None,
    catchup=False,
)

run_spark_hudi_job = SparkSubmitOperator(
    task_id="run_spark_hudi_job",
    application="/opt/airflow/dags/spark_hudi_job.py",
    conn_id="spark_default",
    conf={
        "spark.master": "spark://spark-master:7077",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "1g",
        "spark.executorEnv.AWS_ACCESS_KEY_ID": "admin",
        "spark.executorEnv.AWS_SECRET_ACCESS_KEY": "password",
        "spark.hadoop.fs.s3a.access.key": "admin",
        "spark.hadoop.fs.s3a.secret.key": "password",
        "spark.hadoop.fs.s3a.endpoint": "http://54.221.39.34:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    },
    packages="org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hadoop:hadoop-aws:3.3.4",
    dag=dag,
)

run_spark_hudi_job
