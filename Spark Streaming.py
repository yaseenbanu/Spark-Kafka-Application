from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


KAFKA_TOPIC_NAME = "PRODUCER_TOPIC"
KAFKA_SINK_TOPIC = "CONSUMER_TOPIC"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CHECKPOINT_LOCATION = "C:/pyspark_tmp/kafka_checkpoint/streamer"

spark = SparkSession.builder \
    .appName("SparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

input_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC_NAME)
    .option("startingOffsets", "Earliest")
    .load()
)

input_df = input_df.selectExpr("CAST(value as STRING)", "timestamp")
input_df.printSchema()


sample_schema = (
    StructType()
    .add("col_a", StringType())
    .add("col_b", StringType())
    .add("col_c", StringType())
    .add("col_d", StringType())
)

info_dataframe = input_df.select(
    from_json(col("value"), sample_schema).alias("info"), 
    "timestamp"
)
info_dataframe.printSchema()

info_df_fin = info_dataframe.select("info.*", "timestamp")
info_df_fin.printSchema()

query = info_df_fin.groupBy("col_a").agg(
    approx_count_distinct("col_b").alias("distinct_values_of_col_b"),
    sum("col_c").alias("sum_of_col_c"),
    avg("col_c").alias("average_of_col_c"),
    count("*").alias("total_records")
)

result_1 = query.selectExpr(
    "CAST(col_a AS STRING)",
    "CAST(distinct_values_of_col_b AS STRING)",
    "CAST(sum_of_col_c AS STRING)",
    "CAST(average_of_col_c AS STRING)",
    "CAST(total_records AS STRING)",
)

final_df = result_1.withColumn("value", to_json(struct("*")).cast("string")).select('value')

write_query = (
    final_df.select("value")
    .writeStream.trigger(processingTime="10 seconds")
    .outputMode("complete")
    .format("kafka")
    .option("topic", KAFKA_SINK_TOPIC)
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .start()
)

write_query.awaitTermination()
