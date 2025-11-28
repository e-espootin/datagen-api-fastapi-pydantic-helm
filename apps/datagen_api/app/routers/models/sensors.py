from pydantic import BaseModel
from typing import Dict


class Cobots_Gen(BaseModel):
    sensor_id: str
    temperature: float
    pressure: float
    humidity: float
    reg_timestamp: int
    event_time: str
    loc: Dict = None
    city: str
    country: str

    class Config:
        from_attributes = True


class SCARA_Gen(BaseModel):
    id: str
    temperature: float
    pressure: float
    humidity: float
    timestamp: str
    location: Dict = None
    city: str

    class Config:
        from_attributes = True


class Temperature_Gen(BaseModel):
    sensor_id: str
    temperature: float
    reg_timestamp: int
    event_time: str

    class Config:
        from_attributes = True
