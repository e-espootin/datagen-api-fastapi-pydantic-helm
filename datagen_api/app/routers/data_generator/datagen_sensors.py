import random
from abc import ABC, abstractmethod
from faker import Faker
from random import uniform
from datetime import datetime
from app.routers.models.sensors import Temperature_Gen, Cobots_Gen


class ISensor(ABC):
    @abstractmethod
    def read_data(self) -> dict:
        pass

    def read_batch_data(self, num_samples: int) -> list[dict]:
        pass

    @abstractmethod
    def get_id(self) -> str:
        pass

    @abstractmethod
    def get_topic(self) -> str:
        pass


class TemperatureSensor(ISensor):
    def __init__(self, sensor_id: str, topic_name: str):
        self.sensor_id = sensor_id
        self.topic = topic_name

    async def read_data(self) -> Temperature_Gen:
        # Simulate reading temperature data
        data: Temperature_Gen = {
            "sensor_id": self.sensor_id,
            "temperature": round(random.uniform(1.0, 40.0), 1),
            "reg_timestamp": int(datetime.utcnow().timestamp()),
            "event_time": str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
        }
        return data

    async def read_batch_data(self, num_samples: int) -> list[dict]:
        data = []
        for _ in range(num_samples):
            data.append(
                await self.read_data()
            )
        return data

    def get_id(self) -> str:
        return self.sensor_id

    def get_topic(self) -> str:
        return self.topic


class Cobots(ISensor):
    def __init__(self, sensor_id: str, topic_name: str):
        self.sensor_id = sensor_id
        self.topic = topic_name
        self.fake = Faker()

    async def read_data(self) -> Cobots_Gen:
        data: Cobots_Gen = Cobots_Gen(
            sensor_id=self.sensor_id,
            temperature=round(uniform(20.0, 30.0), 1),
            pressure=round(uniform(1000, 2000), 1),
            humidity=round(uniform(40.0, 60.0), 1),
            reg_timestamp=int(datetime.utcnow().timestamp()),
            event_time=str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
            loc={
                "latitude": round(uniform(-90, 90), 4),
                "longitude": round(uniform(-180, 180), 4),
            },
            city=self.fake.city(),
            country=self.fake.country(),
        )
        return data

    async def read_batch_data(self, num_samples: int) -> list[dict]:
        data = []
        for _ in range(num_samples):
            data.append(
                await self.read_data()
            )
        return data

    def get_id(self) -> str:
        return self.sensor_id

    def get_topic(self) -> str:
        return self.topic
