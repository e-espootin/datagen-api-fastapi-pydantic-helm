from fastapi import APIRouter, HTTPException
from typing import List
from .data_generator.datagen_taxitrip import TaxiTrip_Gen
from .models.taxitrip import TaxiTrip

router = APIRouter()


@router.get("/taxitrips/", tags=["trips"], response_model=List[TaxiTrip])
async def read_users(num_samples: int = 1):
    try:
        return await TaxiTrip_Gen(
            example_file=f'app/routers/sample_data/{'taxitrip'}.csv'
        ).generate_sample_data_faker(num_samples=num_samples)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error generating taxi trips: {str(e)}"
        )
