from xkafka.kafka_consumer import KafkaConsumer
from utils.logger import setup_logger
from dotenv import load_dotenv
import os


#
logger = setup_logger()

# Load environment variables from .env file
load_dotenv()


def main():
    try:
        # read env variables
        topic = os.getenv("TOPIC")

        logger.info("Consumer is running")
        KafkaConsumer().call_consume_topics(topic=topic)
    except Exception as e:
        logger.error(f"Error in main: {e}")
        return None


if __name__ == "__main__":
    main()
