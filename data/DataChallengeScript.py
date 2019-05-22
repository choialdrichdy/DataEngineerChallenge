# This imports all the necessary functions and types from the spark library
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, FloatType, IntegerType
from pyspark.sql.functions import avg, desc, split, col, lag, unix_timestamp, coalesce, sum, lit, count, max, min
from pyspark.sql.window import Window

# This defines the schema for the log file.
# This is taken from https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-entry-format
myManualSchema = StructType(
    [
        StructField("data_timestamp", TimestampType(), True),
        StructField("elb", StringType(), True),
        StructField("client_ip_port", StringType(), True),
        StructField("backend_ip_port", StringType(), True),
        StructField("request_processing_time", FloatType(), True),
        StructField("backend_processing_time", FloatType(), True),
        StructField("response_processing_time", FloatType(), True),
        StructField("elb_status_code", IntegerType(), True),
        StructField("backend_status_code", IntegerType(), True),
        StructField("received_bytes", IntegerType(), True),
        StructField("sent_bytes", IntegerType(), True),
        StructField("request", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("ssl_cipher", StringType(), True),
        StructField("ssl_protocol", StringType(), True),
    ]
)

# This is to read the .log file and use the schema above.
df = spark.read.format("csv").option("delimiter", " ").schema(myManualSchema).load("2015_07_22_mktplace_shop_web_log_sample.log")

# This is the window aggregate function partitions 
window_clients = Window.partitionBy("client_ip").orderBy("data_timestamp")
window_sessions = Window.partitionBy("client_ip","session")

# This separates client_port column (IP Address:Port) into client_ip and client_port
df = df.withColumn("client_ip", split(col("client_ip_port"), ":")[0]).withColumn("client_port", split(col("client_ip_port"), ":")[1])
# This separates request (RequestMethod URL Protocol) into request_method, request_url, and request_protocol
df = df.withColumn("request_method", split(col("request"), " ")[0]).withColumn("request_url", split(col("request"), " ")[1]).withColumn("request_protocol", split(col("request"), " ")[2])
df = df.withColumn("prev_time", lag(df.data_timestamp).over(window_clients))
# This sessionize the DataFrame by computing the difference of the current timestamp to the previous timestamp.
# If the difference is greater than 15 minutes, It is in another session.
df = df.withColumn("session", sum((coalesce((unix_timestamp("data_timestamp") - unix_timestamp("prev_time"))/60, lit(0)) > 15).cast("int")).over(window_clients))
df = df.withColumn("total_session_time", (unix_timestamp(max("data_timestamp").over(window_sessions)) - unix_timestamp(min("data_timestamp").over(window_sessions)))/60)

df.groupBy("client_ip").count().show()

# This is the sample IP address that I used to creating the queries.
test = df.orderBy("client_ip","data_timestamp").where("client_ip = '156.101.9.1'")
test2 = test.withColumn("prev_time", lag(test.data_timestamp).over(window_clients))
test3 = test2.withColumn("session", sum((coalesce((unix_timestamp("data_timestamp") - unix_timestamp("prev_time"))/60, lit(0)) > 15).cast("int")).over(window_clients))
test3.withColumn("total_session_time", (unix_timestamp(max("data_timestamp").over(window_sessions)) - unix_timestamp(min("data_timestamp").over(window_sessions)))/60)
test3.select("client_ip", "session", "request_url").distinct().groupBy("request_url").count().show()

# This is the query to get the average session time.
df.select(avg("total_session_time")).show()
# This is the query to get the most engaged users.
df.select("client_ip","total_session_time").distinct().orderBy(desc("total_session_time")).show()
# This is the query to get the unique URL visits per session
df.select("client_ip", "session", "request_url").distinct().groupBy("request_url").count().show()

