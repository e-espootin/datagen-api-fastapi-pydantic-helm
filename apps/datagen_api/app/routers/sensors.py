from fastapi import APIRouter, HTTPException
from .data_generator.datagen_sensors import TemperatureSensor, Cobots
from .models.sensors import Cobots_Gen, Temperature_Gen

router = APIRouter()


@router.get("/sensors/temp", tags=["sensor_temp"], response_model=Temperature_Gen)
async def read_current_temp(
    sensor_id: str = "temp_1001", topic_name: str = "temp_topic"
):
    try:
        return await TemperatureSensor(
            sensor_id=sensor_id, topic_name=topic_name
        ).read_data()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error getting senor temp: {str(e)}"
        )


@router.get("/sensors/cobot", tags=["sensor_cobot"], response_model=Cobots_Gen)
async def read_sensor_Cobots(
    sensor_id: str = "cobot_1001", topic_name: str = "cobot_topic"
):
    try:
        return await Cobots(sensor_id=sensor_id, topic_name=topic_name).read_data()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error getting senor temp: {str(e)}"
        )
