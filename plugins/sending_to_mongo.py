from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pymongo import MongoClient
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("KafkaJsonConsumerToMongo_PyMongo") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.3.0") \
    .getOrCreate()


def batch_from_kafka(spark):
    try:
        logger.info("Starting Kafka batch reading...")

        raw_data = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "question-producer") \
            .load()

        json_data = raw_data.selectExpr("CAST(value AS STRING) as json_value")

        logger.info("Kafka batch reading initialized successfully.")
        return json_data
    except Exception as e:
        logger.error(f"Error in Kafka batch reading: {str(e)}")
        raise


def df_preprocessing(json_data):
    try:
        logger.info("Starting preprocessing of JSON data...")

        schema = StructType([
            StructField("results", ArrayType(StructType([
                StructField("gender", StringType()),
                StructField("name", StructType([
                    StructField("title", StringType()),
                    StructField("first", StringType()),
                    StructField("last", StringType())
                ])),
                StructField("location", StructType([
                    StructField("street", StructType([
                        StructField("number", IntegerType()),
                        StructField("name", StringType())
                    ])),
                    StructField("city", StringType()),
                    StructField("state", StringType()),
                    StructField("country", StringType()),
                    StructField("postcode", IntegerType()),
                    StructField("coordinates", StructType([
                        StructField("latitude", StringType()),
                        StructField("longitude", StringType())
                    ])),
                    StructField("timezone", StructType([
                        StructField("offset", StringType()),
                        StructField("description", StringType())
                    ]))
                ])),
                StructField("email", StringType()),
                StructField("login", StructType([
                    StructField("uuid", StringType()),
                    StructField("username", StringType())
                ])),
                StructField("dob", StructType([
                    StructField("date", StringType()),
                    StructField("age", IntegerType())
                ])),
                StructField("registered", StructType([
                    StructField("date", StringType()),
                    StructField("age", IntegerType())
                ])),
                StructField("phone", StringType()),
                StructField("cell", StringType())
            ])))
        ])

        parsed_data = json_data.select(
            from_json(col("json_value"), schema).alias("data")
        )
        df_exploded = parsed_data.select(explode(col("data.results")).alias("result"))

        logger.info("JSON data preprocessing completed successfully.")
        return df_exploded
    except Exception as e:
        logger.error(f"Error in preprocessing JSON data: {str(e)}")
        raise


def insert_into_mongo(df_exploded):
    try:
        logger.info("Inserting data into MongoDB using pymongo...")

        # Spark DataFrame -> list of dicts
        records = df_exploded.select("result").rdd.map(lambda row: row["result"].asDict(recursive=True)).collect()

        # MongoDB bağlantısı
        client = MongoClient("mongodb://root:example@mongodb:27017")
        db = client["customers"]
        collection = db["user_information"]

        if records:
            collection.insert_many(records)
            logger.info(f"{len(records)} documents inserted into MongoDB.")
        else:
            logger.info("No records to insert.")

    except Exception as e:
        logger.error(f"Error inserting into MongoDB: {str(e)}")
        raise


def processing_data_mongo():
    try:
        logger.info("Starting batch data processing...")
        json_data = batch_from_kafka(spark)
        df_exploded = df_preprocessing(json_data)
        insert_into_mongo(df_exploded)
        logger.info("Batch data processing completed.")
    except Exception as e:
        logger.error(f"Batch processing error: {str(e)}")
        raise


if __name__ == '__main__':
    processing_data_mongo()
