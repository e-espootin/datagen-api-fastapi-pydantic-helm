from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import os
import pandas as pd
from dotenv import load_dotenv
import socket
from datetime import datetime
from upload.s3_uploader import S3Uploader
from utils.logger import setup_logger
import time

#
logger = setup_logger()

# Load environment variables from .env file
load_dotenv()


class KafkaConsumer:
    def __init__(self):
        self.consumer = None
        self.group_id = os.getenv("GROUP_ID", "my_group_id")
        # self.admin_client = None
        self.get_conn_from_pool()
        self.s3_uploader = S3Uploader()
        # variables
        self.messages_batch_size = os.getenv("MESSAGES_BATCH_SIZE", 10)
        self.sleep_time = os.getenv("SLEEP_TIME_SEC", 5)

    def is_port_open(self, host, port):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                logger.info(f"Checking port {port} on host {host}")
                sock.settimeout(1)
                result = sock.connect_ex((host, port))
                return result == 0
        except Exception as e:
            logger.error(e)
            return False

    def get_conn_from_pool(self):
        try:
            conns = {
                "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
                "KAFKA_BOOTSTRAP_B1": os.getenv("KAFKA_BOOTSTRAP_B1"),
                "KAFKA_BOOTSTRAP_B2": os.getenv("KAFKA_BOOTSTRAP_B2"),
                "KAFKA_BOOTSTRAP_DEFAULT": "localhost:9092",
                "KAFKA_def_kubernetes": f"{os.getenv("KAFKA_RELEASE")}.dev.svc.cluster.local:9092",
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
                                f"Port {port} on host {host} is not accessible."
                            )
                            continue

                        #
                        self.consumer = Consumer(
                            {
                                "bootstrap.servers": conn,
                                "group.id": self.group_id,
                                "auto.offset.reset": "earliest",
                                "enable.auto.commit": False,  # Manual commit for better control
                                "session.timeout.ms": 10000,
                                # "retries": 0,
                                "retry.backoff.ms": 500,
                                "max.poll.interval.ms": 300000,  # 5 minutes
                            }
                        )

                        metadata = self.consumer.list_topics(timeout=10)
                        logger.debug(f"Kafka topics list: {metadata}")
                        return True
                    except Exception as e:
                        logger.error(e)
                        continue

            return None
        except Exception as e:
            logger.error(e)
            return None

    def consume_messages(self, topic, timeout=1.0, messages_batch_size: int = 10):
        '''Consume messages from Kafka topic and export to S3'''
        try:

            if self.consumer is None:
                logger.error("Consumer is not initialized (consume_messages)")
                raise Exception("Consumer is not initialized")

            self.consumer.subscribe([topic])
            messages = []

            try:
                while True:
                    msg = self.consumer.poll(timeout)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.info(
                                f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
                            )
                        elif msg.error():
                            raise KafkaException(msg.error())
                    else:
                        # Process the message
                        # logger.info(json.loads(msg.value().decode("utf-8")))
                        for item in json.loads(msg.value().decode("utf-8")):
                            if type(item) is not dict:
                                continue
                            messages.append(item)
                        logger.debug(
                            f"message length: {len(messages)}, time is: {datetime.now()}"
                        )
                        # Commit offset after processing
                        self.consumer.commit(msg)

                    # export messages to parquet
                    if len(messages) >= messages_batch_size:

                        df = pd.DataFrame(messages)
                        logger.debug(df.info())
                        # logger.info(messages)
                        # estimate size
                        memory_bytes = df.memory_usage(deep=True).sum()
                        memory_mb = memory_bytes / (1024**2)
                        logger.info(f"DataFrame size: {memory_mb:.2f} MB")
                        # yield messages
                        self.s3_uploader.export_messages_datalake_S3_parquet(
                            df=df, topic=topic
                        )
                        messages = []
                        break

            except KeyboardInterrupt:
                logger.warning("Keyboard interrupt in consume_messages")

        except Exception as e:
            logger.error(f"Failed to consume messages: {e}")
            raise e

    def get_all_topics(self) -> list:
        """Get all topics from Kafka broker"""
        try:
            if self.consumer is None:
                logger.error("Consumer is not initialized (get_all_topics)")
                raise Exception("Consumer is not initialized")

            metadata = self.consumer.list_topics(timeout=10)
            topics = metadata.topics
            return list(topics.keys())
        except KafkaException as e:
            logger.error(f"Failed to get topics: {e}")
            return []

    def call_consume_topics(self, topic: str = None) -> True:
        """Consume multi or one messages from Kafka topics"""
        try:
            #
            logger.info("getting topics list")
            if topic is None or topic == "":
                topics = self.get_all_topics()
                logger.info("Topics has been read!")
            #
            while True:
                if topic is None or topic == "":
                    for item in topics:
                        if item == "__consumer_offsets":
                            continue
                        logger.info(f"Consuming messages from topic: {item}")
                        self.consume_messages(
                            topic=item,
                            messages_batch_size=int(self.messages_batch_size),
                        )
                else:
                    logger.info(f"start consume topic(el): {topic}")
                    self.consume_messages(
                        topic=topic, messages_batch_size=int(
                            self.messages_batch_size)
                    )
                # sleep time
                time.sleep(int(self.sleep_time))
                logger.info(
                    f"Sleeping for {self.sleep_time}, current time is: {time.ctime()}"
                )

        except KeyboardInterrupt:
            logger.warning("Keyboard interrupt in call_consume_topics")
            pass
        except Exception as e:
            logger.error(f"Failed to consume messages: {e}")
            raise e
        finally:
            self.consumer.close()

    @staticmethod
    def delivery_report(err, msg):
        """Delivery callback to handle success or error after sending a message."""
        try:
            if err is not None:
                logger.warning(f"Message delivery failed: {err}")
            else:
                logger.error(
                    f"Message delivered to {msg.topic()} [{msg.partition()}]")
        except Exception as e:
            logger.error(e)

    def close(self):
        try:
            self.producer.flush()
        except Exception as e:
            logger.error(e)
            pass
