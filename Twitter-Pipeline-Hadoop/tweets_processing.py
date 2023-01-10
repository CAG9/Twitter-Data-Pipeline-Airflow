from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Tweets processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .config("spark.driver.memory","4g")\
    .getOrCreate()

# Read the file forex_rates.json from the HDFS
df = spark.read.csv('hdfs://namenode:9000//SismologicoMxtweets/SismologicoMX_twitter_data.csv',header=True)

# Rename user column to twitter_account
df = df.withColumnRenamed('user','twitter_account')

#df.printSchema()
# Drop the duplicated rows based on the base and last_update columns
tweets = df.select('created_at', 'twitter_account', 'text', 'favorite_count', 'retweet_count')
#    .dropDuplicates(['created_at'])
#tweets.show()
# Export the dataframe into the Hive table forex_rates
tweets.write.mode("append").insertInto("sismologicomx_tweets")
