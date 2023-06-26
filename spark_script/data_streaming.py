import signal
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date, current_timestamp

CASSANDRA_HOST = 'cassandra'
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = 'sales'
CASSANDRA_TABLE = 'orders'

MYSQL_HOST = 'mysql'
MYSQL_PORT = 3306
MYSQL_DATABASE = 'sales'
MYSQL_TABLE = 'aggregated_sales'
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = 'root'

KAFKA_BOOTSTRAP_SERVER = 'kafka:9092'
KAFKA_TOPIC = 'sales_orders'

def write_to_cassandra(df, epoch_id):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE) \
        .mode("append") \
        .save()

def write_to_mysql(df, epoch_id):
    agg_df = df.withColumn("order_date", to_date(col("created_at"))) \
        .groupBy("order_date", "platform_id", "product_id") \
        .agg(sum("quantity").alias("total_quantity")) \
        .withColumn("processed_at", current_timestamp())

    agg_df.write \
        .jdbc(url=f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}", 
                table=MYSQL_TABLE, 
                mode="append", 
                properties= {
                    "driver": "com.mysql.cj.jdbc.Driver",
                    "user": MYSQL_USERNAME,
                    "password": MYSQL_PASSWORD}) 

def signal_handler(signal, frame):
    print("Waiting for 90 seconds before terminating the Spark Streaming application...")
    time.sleep(90)
    sys.exit(0)
        
def main():
    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra-MySQL") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load() \
        .createOrReplaceTempView("tmp_table")
        
    query = """
        SELECT FROM_JSON(
                CAST(value AS STRING), 
                    'order_id INT, 
                    created_at TIMESTAMP, 
                    platform_id INT, 
                    product_id INT, 
                    quantity INT,
                    customer_id INT,
                    payment_method STRING'
                ) AS json_struct 
        FROM tmp_table
    """
    
    tmp_df = spark.sql(query).select("json_struct.*")
    
    tmp_df.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start() \
            
    tmp_df.writeStream \
        .foreachBatch(write_to_mysql) \
        .outputMode("append") \
        .trigger(processingTime='60 seconds') \
        .start() 
    
    signal.signal(signal.SIGINT, signal_handler)
    spark.streams.awaitAnyTermination()
    
if __name__ == "__main__":
    main()


