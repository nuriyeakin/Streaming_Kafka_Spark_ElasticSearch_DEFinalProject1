import findspark
import warnings
from elasticsearch import Elasticsearch, helpers

findspark.init("/opt/manual/spark")
warnings.simplefilter(action='ignore')

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Read_From_Kafka") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1") \
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

#Read from kafka office-input

lines= (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers","localhost:9092")
        .option("subscribe","office-input")
        .load())

lines2 = lines.selectExpr("CAST(value AS STRING)")

lines3 = lines2.withColumn("event_ts_min", F.to_date(F.trim((F.split(F.col("value"),",")[0])),'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("ts_min_bignt",F.trim((F.split(F.col("value"),",")[1])).cast("integer")) \
    .withColumn("room",F.trim((F.split(F.col("value"),",")[2])).cast("string")) \
    .withColumn("co2",F.trim((F.split(F.col("value"),",")[3])).cast("double")) \
    .withColumn("light",F.trim((F.split(F.col("value"),",")[4])).cast("double")) \
    .withColumn("temp",F.trim((F.split(F.col("value"),",")[5])).cast("double")) \
    .withColumn("humidity",F.trim((F.split(F.col("value"),",")[6])).cast("double")) \
    .withColumn("pir",F.trim((F.split(F.col("value"),",")[7])).cast("double")) \
    .drop("value","key","topic","partition","offset","timestamp")

#lines3.printSchema()

#Write to Elasticsearch

query = lines3.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes","localhost") \
    .option("es.port","9200") \
    .option("checkpointLocation","file:///tmp/streaming/final_write") \
    .start("online-final-spark")

query.awaitTermination()
