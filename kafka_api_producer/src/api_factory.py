from api_sources.local_api import LocalApi
from kafka.kafka_producer import KafkaSensorProducer
import os
import json
import time
from dotenv import load_dotenv
from utils.logger import setup_logger

logger = setup_logger()
load_dotenv()


class ApiFactory:
    def __init__(self):
        self.api_local_url = os.getenv("API_LOCAL_URL")
        self.api_list = None
        self.api_local = LocalApi()
        self.kafka_producer = KafkaSensorProducer()
        self.sleep_time = os.getenv("SLEEP_TIME_SEC", 5)

    def get_list(self, dsname: str) -> dict:
        try:
            # read from env
            ds = json.loads(
                os.getenv(
                    "ALL_DATASETS", '{"taxitrips": "/taxitrips/?num_samples=1"}'
                ).strip()
            )
            logger.info(f"Raw datasets from env: {ds}")
            if ds is not None:
                return {dsname: ds[dsname]}
            else:
                return {}
        except Exception as e:
            logger.error(f"Error in get_list: {e}")
            raise e

    def list_handler(self, dsname: str):
        try:
            api_list = self.get_list(dsname=dsname)
            logger.info(f"api_local_url: {self.api_local_url}")
            logger.info(f"API list: {api_list}")

            for api_name, api_url in api_list.items():
                try:
                    while True:  # infinite loop
                        # call api
                        res = self.api_local.get_data(
                            f"{self.api_local_url}{api_url}")

                        # send data to kafka
                        self.kafka_producer.send(api_name, res)

                        # sleep time
                        time.sleep(int(self.sleep_time))
                        logger.info(
                            f"Sleeping for {self.sleep_time}, dataset is: {api_name} , current time is: {time.ctime()}"
                        )

                except Exception as e:
                    logger.error(
                        f"Error in sending data to topic: {api_name} error: {e}"
                    )
        except Exception as e:
            logger.error(f"Error in list_handler: {e}")
            raise e
