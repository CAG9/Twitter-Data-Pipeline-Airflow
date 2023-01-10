import airflow
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from datetime import datetime, timedelta
##################################################################################
import pandas
import requests
import json
###################################################################################
from twitter_extract import twitter_extract

default_args = {
            "owner": "Airflow",
            "start_date": airflow.utils.dates.days_ago(10),
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "email": "youremail@host.com",
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        }

with DAG(dag_id="twitter_data_pipeline_hdfs", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    # Downloading tweets
    extract_tweets = PythonOperator(
    task_id='extract_tweets',
    python_callable=twitter_extract,
    dag=dag,
    )

    #check if a file exist in a specific location
    is_twitter_file_available = FileSensor(
    task_id="is_twitter_file_available",
    fs_conn_id="twitter_path",
    filepath="SismologicoMX_twitter_data.csv",
    poke_interval=5,
    timeout=20
    )

    # Saving tweets in HDFS
    saving_tweets = BashOperator(
        task_id="saving_tweets",
        bash_command="""
            hdfs dfs -mkdir -p /SismologicoMxtweets && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/SismologicoMX_twitter_data.csv /SismologicoMxtweets
            """
    )

    # Creating a hive table named sismologicomx_tweets
    creating_tweets_table = HiveOperator(
        task_id="creating_tweets_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS sismologicomx_tweets(
                created_at DATE,
                twitter_account STRING,
                text STRING,
                favorite_count DOUBLE,
                retweet_count DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    # Running Spark Job to process the data
    tweets_processing = SparkSubmitOperator(
        task_id="tweets_processing",
        conn_id="spark_conn",
        application="/opt/airflow/dags/scripts/tweets_processing.py",
        verbose=False
    )    

    # Sending a notification by email
    # https://stackoverflow.com/questions/51829200/how-to-set-up-airflow-send-email
    sending_email_notification = EmailOperator(
            task_id="sending_email_notification",
            to="racec9999@gmail.com",
            subject="Twitter_SismologicoMX_data_pipeline",
            html_content="""
                <h3>Twitter_SismologicoMX_data_pipeline succeeded</h3>
            """
            )


    extract_tweets >> is_twitter_file_available >> saving_tweets >> creating_tweets_table >> tweets_processing >> sending_email_notification

















