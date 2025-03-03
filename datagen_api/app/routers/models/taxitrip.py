from pydantic import BaseModel, EmailStr, Field
from datetime import datetime
from enum import Enum
from typing import Optional


class WeatherDescription(str, Enum):
    CLEAR = "Clear"
    CLOUDY = "Cloudy"
    RAIN = "Rain"
    SNOW = "Snow"
    FOG = "Fog"


class TripSatisfaction(str, Enum):
    EXCELLENT = "Excellent"
    GOOD = "Good"
    AVERAGE = "Average"
    POOR = "Poor"


class DriverInfo(BaseModel):
    email: EmailStr
    phone_number: str
    fullname: str
    credit_card: str


class PassengerInfo(BaseModel):
    email: EmailStr
    phone_number: str
    fullname: str
    credit_card: str
    address: str
    job: str
    age: int = Field(ge=18, le=60)
    sex: str = Field(pattern="^[MF]$")


class GeoLocation(BaseModel):
    latitude: float = Field(ge=40.5, le=40.9)
    longitude: float = Field(ge=-74.5, le=-73.5)


class WeatherInfo(BaseModel):
    aqi: int = Field(ge=20, le=80)
    temperature: int = Field(ge=-10, le=45)
    humidity: int = Field(ge=0, le=100)
    precipitation_chance: float = Field(ge=0, le=100)
    uv_index: int = Field(ge=0, le=100)
    feels_like: int = Field(ge=0, le=100)
    description: str
    wind_speed_km: int = Field(ge=2, le=35)


class TaxiTrip(BaseModel):
    vendor_id: int = Field(ge=1, le=2)
    log_datetime: datetime
    lpep_pickup_datetime: datetime
    lpep_dropoff_datetime: Optional[datetime] = None
    request_datetime: datetime
    passenger_count: int = Field(ge=1, le=4)
    trip_distance: float
    ratecode_id: int
    store_and_fwd_flag: str
    pu_location_id: int
    do_location_id: int
    payment_type: int
    fare_amount: float
    extra: float
    mta_tax: float = Field(ge=0.5, le=1.0)
    tip_amount: float = Field(ge=0, le=20)
    tolls_amount: float
    improvement_surcharge: float
    total_amount: float = Field(ge=5, le=100)
    congestion_surcharge: float
    airport_fee: float = Field(ge=5, le=100)

    # Driver and passenger information
    driver: DriverInfo
    passenger: PassengerInfo

    # Location information
    pickup_location: GeoLocation
    dropoff_location: GeoLocation

    # Weather and environmental information
    pickup_weather: WeatherInfo
    dropoff_weather: WeatherInfo

    # Trip satisfaction
    trip_satisfaction: TripSatisfaction
    #
    lpep_dropoff_datetime: datetime
    request_datetime: datetime

    class Config:
        from_attributes = True
