from .base_api import BaseAPI
import requests


class LocalApi(BaseAPI):
    def __init__(self):
        pass

    def get_data(self, url: str) -> dict:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception("Failed to fetch data from the API")
        except Exception as e:
            print(f"Error: {e}")
            raise
