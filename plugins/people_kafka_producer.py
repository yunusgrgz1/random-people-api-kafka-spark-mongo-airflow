from pyspark.sql import SparkSession
import json
import logging
import requests
from datetime import datetime

# Kafka Producer configuration
producer_params = {
    'kafka.bootstrap.servers': 'broker:29092',
    'topic': 'question-producer'
}

# Fetch data from API
num_of_results = 30
url = f"https://randomuser.me/api/?results={num_of_results}"

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def send_to_kafka(spark, data):
    try: 
        spark.createDataFrame([(json.dumps(data),)], ['value']) \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", producer_params['kafka.bootstrap.servers']) \
            .option("topic", producer_params['topic']) \
            .save()
        
        logger.info("Data sent to Kafka.")
    except Exception as e:
        logger.error(f"Error sending data to Kafka: {e}")

def fetch_and_produce():
    # Create Spark session here
    spark = SparkSession.builder \
        .appName("KafkaProducerExample") \
        .getOrCreate()
    
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            logger.error(f"API error {response.status_code}")
            return

        persons = response.json()
        send_to_kafka(spark, persons)

        for person in persons['results']:
            first_name = person['name']['first']
            last_name = person['name']['last']
            logger.info(f"Sent person {first_name} {last_name}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error with API {e}")
    except Exception as e:
        logger.error(f"Unexpected error {e}")
