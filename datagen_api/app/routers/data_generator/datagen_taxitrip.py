from faker import Faker
import random
import pandas as pd
import os
from app.routers.models.taxitrip import TaxiTrip


class Config:
    from_attributes = True


class TaxiTrip_Gen:
    def __init__(
        self, example_file="app/routers/data_generator/taxi_tripdata_top1000.csv"
    ):
        self.example_file = example_file
        self.df = self.validate_sample_file()
        self.fake = Faker()

    # load sample data

    def validate_sample_file(self) -> pd.DataFrame:
        try:
            if not os.path.exists(self.example_file):
                print(
                    f"File not exists in dir : {self.example_file} project root dir: {os.getcwd()}"
                )

            df = pd.read_csv(self.example_file)
            return df
        except FileNotFoundError:
            print(f"File not found: {self.example_file}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()

    # generate using Faker library
    async def generate_sample_data_faker(self, num_samples=1) -> list[TaxiTrip]:
        try:
            if self.df is None or self.df.empty:
                print("df is empty")
                return False

            # get one random row from the dataset
            sampled_rand_row = self.df.sample(n=1, random_state=42).to_dict(
                orient="records"
            )[0]
            sample_data: list[TaxiTrip] = []

            for _ in range(num_samples):
                record: TaxiTrip = {
                    "vendor_id": random.randint(1, 2),
                    "log_datetime": self.fake.date_time_this_decade().isoformat(),
                    "lpep_pickup_datetime": self.fake.date_time_this_decade().isoformat(),
                    "lpep_dropoff_datetime": "",
                    "request_datetime": "",
                    "passenger_count": random.randint(1, 4),
                    "trip_distance": sampled_rand_row["trip_distance"],
                    "ratecode_id": sampled_rand_row["RatecodeID"],
                    "store_and_fwd_flag": sampled_rand_row["store_and_fwd_flag"],
                    "pu_location_id": sampled_rand_row["PULocationID"],
                    "do_location_id": sampled_rand_row["DOLocationID"],
                    "payment_type": sampled_rand_row["payment_type"],
                    "fare_amount": sampled_rand_row["fare_amount"],
                    "extra": sampled_rand_row["extra"],
                    "mta_tax": sampled_rand_row["mta_tax"]
                    if sampled_rand_row["mta_tax"]
                    else random.uniform(0.5, 1.0),
                    "tip_amount": random.uniform(0, 20),
                    "tolls_amount": sampled_rand_row["tolls_amount"],
                    "improvement_surcharge": sampled_rand_row["improvement_surcharge"],
                    "total_amount": random.uniform(5, 100),
                    "congestion_surcharge": sampled_rand_row["congestion_surcharge"],
                    "airport_fee": random.uniform(5, 100),
                    # add some irrelevant sensitive data for test purposes - driver
                    "driver": {
                        "email": self.fake.email(),
                        "phone_number": self.fake.phone_number(),
                        "fullname": self.fake.name(),
                        "credit_card": self.fake.credit_card_number(),
                    },
                    # add some irrelevant sensitive data for test purposes - passenger
                    "passenger": {
                        "email": self.fake.email(),
                        "phone_number": self.fake.phone_number(),
                        "fullname": self.fake.name(),
                        "credit_card": self.fake.credit_card_number(),
                        "address": str.replace(
                            str.replace(self.fake.address(), ",", " - "), "\n", " "
                        ),
                        "job": self.fake.job(),
                        "age": random.randint(18, 60),
                        "sex": random.choice(["M", "F"]),
                    },
                    # imaginery geolocation data in New York city
                    "pickup_location": {
                        "latitude": random.uniform(40.5, 40.9),
                        "longitude": random.uniform(-74.5, -73.5),
                    },
                    "dropoff_location": {
                        "latitude": random.uniform(40.5, 40.9),
                        "longitude": random.uniform(-74.5, -73.5),
                    },
                    # Air pollution quality data for NY
                    "pickup_weather": {
                        "aqi": random.randint(20, 80),
                        "temperature": random.randint(-10, 45),
                        "humidity": random.randint(0, 100),
                        "precipitation_chance": random.uniform(0, 100),
                        "uv_index": random.randint(0, 100),
                        "feels_like": random.randint(0, 100),
                        "description": random.choice(
                            ["Clear", "Cloudy", "Rain", "Snow", "Fog"]
                        ),
                        "wind_speed_km": random.randint(2, 35),
                    },
                    "dropoff_weather": {
                        "aqi": random.randint(20, 80),
                        "temperature": random.randint(-10, 45),
                        "humidity": random.randint(0, 100),
                        "precipitation_chance": random.uniform(0, 100),
                        "uv_index": random.randint(0, 100),
                        "feels_like": random.randint(0, 100),
                        "description": random.choice(
                            ["Clear", "Cloudy", "Rain", "Snow", "Fog"]
                        ),
                        "wind_speed_km": random.randint(2, 35),
                    },
                    # given trip score
                    "trip_satisfaction": random.choice(
                        ["Excellent", "Good", "Average", "Poor"]
                    ),
                }
                record["lpep_dropoff_datetime"] = (
                    pd.to_datetime(record["lpep_pickup_datetime"])
                    + pd.to_timedelta(random.randint(1, 60), unit="m")
                ).isoformat()
                record["request_datetime"] = (
                    pd.to_datetime(record["lpep_pickup_datetime"])
                    - pd.to_timedelta(random.randint(1, 120), unit="m")
                ).isoformat()

                sample_data.append(record)

            return sample_data
        except Exception as e:
            print(f"Error generating data: {e}")
            return []
