from pyspark.sql import SparkSession
import logging
import boto3
import os

logging.basicConfig(level=logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')


def spark_session():
    spark = SparkSession.builder \
        .appName("KafkaJsonConsumerToS3") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.3.0") \
        .getOrCreate()
    return spark

def read_json_from_kafka(spark):
    logging.info("Reading JSON data from Kafka (batch)...")
    raw_data = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "question-producer") \
        .load()
    
    json_data = raw_data.selectExpr("CAST(value as STRING) as json_value", "timestamp AS event_time")
    return json_data

def upload_json_to_s3(json_data):
    logging.info("Converting Spark DataFrame to JSON string...")
    try:
        json_lines = json_data.select("json_value").rdd.map(lambda row: row.json_value).collect()
        json_str = '\n'.join(json_lines)
    except Exception as e:
        logging.error(f"❌ Data conversion error: {e}")
        return
    
    logging.info("Connecting to S3..")
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_acces_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )

    try:
        logging.info("Uploading JSON to S3...")
        s3.put_object(
            Bucket= os.getenv("S3_BUCKET_NAME"),
            Key='raw_data/raw_json_data.json',
            Body= json_str,
            ContentType='application/json'
        )
        logging.info("✅ Successfully uploaded JSON to S3.")
    except Exception as e:
        logging.error(f"❌ Failed to upload to S3: {e}")


def start_sending_to_s3(spark):
    json_data = read_json_from_kafka(spark)
    upload_json_to_s3(json_data)
    

# Run the process
if __name__ == "__main__":
    start_sending_to_s3()
