# Mexican National Seismological Service(Twitter) Data Pipeline Airflow

## Data pipeline Hadoop (Big data)
* Extract tweets from the Mexican National Seismological Service account with the Twitter api and Tweepy and save it in a csv file
* Check if the file has been created correctly
* Move the csv file to hdfs
* Create a hive table for storing data
* Create a PySpark script to process and insert the data into the hive table
* Send an email notification when the data pipeline is completed

Orchestrated by Airflow
## Data pipeline Lite
Extract tweets from Mexican National Seismological Service and stored in  an amazon s3 bucket, all running in an EC2 instance.
## Results
- DAG:
![Dag](https://github.com/CAG9/Twitter-Data-Pipeline-Airflow/blob/main/Twitter-Pipeline-Hadoop/Dag.png)
- Hive table: 
![Hive table](https://github.com/CAG9/Twitter-Data-Pipeline-Airflow/blob/main/Twitter-Pipeline-Hadoop/Hive.png)


## Tools and Technologies
- Python 3
- Pyspark
- AWS
- Hadoop
- HDFS
- Hive
- Airflow
- Datetime
- Pandas
- Requests
- Json
- Tweepy
- s3fs

