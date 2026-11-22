from typing import Dict, List
import requests

class CourierReader:

    def get_couriers(self, limit, offset):

        url = (
            "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/"
            "couriers"
        )

        headers = {
            "X-Nickname": "AnrdeyPeshikov",
            "X-Cohort": "19",
            "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"
        }

        params = {
            "sort_field": "_id",
            "sort_direction": "asc",
            "limit": limit,
            "offset": offset
        }

        response = requests.get(
            url=url,
            headers=headers,
            params=params
        )

        response.raise_for_status()

        return response.json()