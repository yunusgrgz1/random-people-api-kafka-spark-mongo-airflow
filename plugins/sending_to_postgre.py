from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType
import psycopg2
import logging


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Spark session initialization
spark = SparkSession.builder \
    .appName("KafkaJsonConsumerToPostgre") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.3.0") \
    .getOrCreate()

def batch_from_kafka(spark):
    """
    Function to read batch data from Kafka.
    """
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
    """
    Parse JSON and explode results array.
    """
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
            ])))]) 

        parsed_data = json_data.select(
            from_json(col("json_value"), schema).alias("data")
        )
        df_exploded = parsed_data.select(explode(col("data.results")).alias("result"))

        logger.info("JSON data preprocessing completed successfully.")
        return df_exploded
    except Exception as e:
        logger.error(f"Error in preprocessing JSON data: {str(e)}")
        raise

def spark_processing(df_exploded):
    """
    Select and flatten required columns.
    """
    try:
        logger.info("Starting Spark DataFrame processing...")

        processed_df = df_exploded.select(
            col("result.gender").alias("gender"),
            col("result.name.title").alias("title"),
            col("result.name.first").alias("first"),
            col("result.name.last").alias("last"),
            col("result.location.street.name").alias("street_name"),
            col("result.location.street.number").alias("street_number"),
            col("result.location.city").alias("city"),
            col("result.location.state").alias("state"),
            col("result.location.country").alias("country"),
            col("result.location.postcode").alias("postcode"),
            col("result.email").alias("email"),
            col("result.login.username").alias("username"),
            col("result.phone").alias("phone"),
            col("result.cell").alias("cell")
        )

        logger.info("Spark DataFrame processing completed successfully.")
        return processed_df
    except Exception as e:
        logger.error(f"Error in Spark DataFrame processing: {str(e)}")
        raise

def inserting_into_postgre(df):
    """
    Handle the batch data to insert into PostgreSQL.
    """
    try:
        logger.info("Processing batch data...")

        pandas_df = df.toPandas()

        if pandas_df.empty:
            logger.info("No data in batch.")
            return

        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
        )
        cursor = conn.cursor()

        for _, row in pandas_df.iterrows():
            cursor.execute("""
                INSERT INTO "users" (
                    gender, first_name, last_name, title, email, username
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['gender'], row['first'], row['last'], row['title'],
                row['email'], row['username']
            ))

            cursor.execute("""
                INSERT INTO "addresses" (
                    street_name, street_number, city, state, country, postcode
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['street_name'], row['street_number'], row['city'], row['state'],
                row['country'], row['postcode']
            ))

            cursor.execute("""
                INSERT INTO "logins" (username)
                VALUES ( %s)
            """, (row['username'],))

            cursor.execute("""
                INSERT INTO "contacts" (phone, cell)
                VALUES (%s, %s)
            """, (row['phone'], row['cell']))

        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Batch processed successfully.")
    except Exception as e:
        logger.error(f"Error processing batch data: {str(e)}")
        if conn:
            conn.rollback()
        raise

def processing_data():
    """
    Main function to setup and start the batch processing.
    """
    try:
        logger.info("Starting batch data processing...")
        json_data = batch_from_kafka(spark)
        df_exploded = df_preprocessing(json_data)
        processed_df = spark_processing(df_exploded)

        inserting_into_postgre(processed_df)
        
        logger.info("Batch data processing completed.")
    except Exception as e:
        logger.error(f"Batch processing error: {str(e)}")
        raise

if __name__ == '__main__':
    processing_data()
