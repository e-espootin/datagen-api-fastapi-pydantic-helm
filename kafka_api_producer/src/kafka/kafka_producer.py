from confluent_kafka import Producer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient

# from .kafka_config import DEFAULT_TOPIC_CONFIG
import json
import os
from dotenv import load_dotenv
import socket
from utils.logger import setup_logger

logger = setup_logger()
load_dotenv()


class KafkaSensorProducer:
    def __init__(self):
        self.producer = None
        self.admin_client = None
        self.get_conn_from_pool()

    def is_port_open(self, host, port):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                logger.info(f"Checking port {port} on host {host}")
                sock.settimeout(1)
                result = sock.connect_ex((host, port))
                return result == 0
        except Exception as e:
            logger.error(e)
            raise e

    def get_conn_from_pool(self):
        try:
            conns = {
                "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
                "KAFKA_BOOTSTRAP_B1": os.getenv("KAFKA_BOOTSTRAP_B1"),
                "KAFKA_BOOTSTRAP_B2": os.getenv("KAFKA_BOOTSTRAP_B2"),
                "KAFKA_BOOTSTRAP_DEFAULT": "localhost:9092",
                "KAFKA_def_kubernetes": f'{os.getenv("KAFKA_RELEASE")}.dev.svc.cluster.local:9092',
                "KAFKA_def_kubernetes_ext": "kafka-plain.dev.svc.cluster.local:9092",
            }
            for k, v in conns.items():
                if v is not None:
                    try:
                        conn = v
                        logger.info(f"kafka server: {conn}")

                        host, port = conn.split(":")
                        if not self.is_port_open(host, int(port)):
                            logger.info(
                                f"Port {port} on host {host} is not accessible.")
                            continue
                        #
                        self.producer = Producer(
                            {"bootstrap.servers": conn, "retries": 0}
                        )
                        self.admin_client = AdminClient(
                            {"bootstrap.servers": conn, "retries": 0}
                        )

                        metadata = self.producer.list_topics(timeout=10)
                        logger.debug(f"Kafka topics list: {metadata}")
                        return True
                    except Exception as e:
                        logger.error(e)
                        continue

            return None
        except Exception as e:
            logger.error(e)
            return None

    # topic creation
    # def create_topic(self, topic: str):
    #     """
    #     Create a Kafka topic if it doesn't exist.

    #     :param topic: Name of the Kafka topic.
    #     """
    #     existing_topics = set(
    #         self.admin_client.list_topics(timeout=5).topics.keys())
    #     if topic in existing_topics:
    #         logger.info(f"Topic '{topic}' already exists.")
    #         return

    #     # Define the new topic
    #     new_topic = NewTopic(
    #         topic,
    #         num_partitions=DEFAULT_TOPIC_CONFIG["num_partitions"],
    #         replication_factor=DEFAULT_TOPIC_CONFIG["replication_factor"]
    #     )

    #     # Try to create the topic
    #     try:
    #         self.admin_client.create_topics([new_topic])
    #         print(f"Created topic: {topic}")
    #     except KafkaException as e:
    #         print(f"Failed to create topic {topic}: {e}")

    # send data
    def send(self, topic_name: str, data: dict):
        """
        Send data to the Kafka topic corresponding to the sensor type.

        :param sensor_type: The type of sensor (e.g., 'temperature', 'humidity', 'gps').
        :param data: The data to send to the Kafka topic.
        """
        try:
            # topic = KAFKA_TOPICS[topic_name]
            payload = json.dumps(data).encode("utf-8")

            self.producer.produce(topic_name, payload,
                                  callback=self.delivery_report)
            self.producer.flush()  # Ensure all messages are sent
            # print(f"Sent to {topic_name}: {data}")
        except ValueError as e:
            logger.error(f"ValueError: {e}")
            logger.error(
                f"Topic '{topic_name}' does not exist. Creating topic...")
            # self.create_topic(topic_name)
            raise e

        except KafkaException as e:
            # Check for UNKNOWN_TOPIC_OR_PART error
            if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                logger.error(
                    f"Topic '{topic_name}' does not exist. Creating topic...")
                # self.create_topic(topic_name)
            # UNKNOWN_TOPIC_ID
            #
            else:
                logger.error(
                    f"Failed to send message to {topic_name}: {str(e)}")
            raise e
        except Exception as e:
            logger.error(f"Failed to send message to {topic_name}: {e}")
            raise e

    @staticmethod
    def delivery_report(err, msg):
        """Delivery callback to handle success or error after sending a message."""
        try:
            if err is not None:
                logger.warning(f"Message delivery failed: {err}")
            else:
                logger.info(
                    f"Message delivered to {msg.topic()} [{msg.partition()}]")
        except Exception as e:
            logger.error(f"Failed to close producer: {e}")
            pass

    def close(self):
        try:
            self.producer.flush()
        except Exception as e:
            logger.error(f"Failed to close producer: {e}")
            pass
