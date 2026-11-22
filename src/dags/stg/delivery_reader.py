import requests
from datetime import datetime

class DeliveryReader:

    def get_deliveries(self, from_date:datetime, limit, offset):

        url = (
            "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/"
            "deliveries"
        )

        headers = {
            "X-Nickname": "AnrdeyPeshikov",
            "X-Cohort": "19",
            "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"
        }

        params = {
            "sort_field": "order_ts",
            "sort_direction": "asc",
            "limit": limit,
            "offset": offset,
            "from": from_date.strftime('%Y-%m-%d %H:%M:%S')
        }

        response = requests.get(
            url=url,
            headers=headers,
            params=params
        )

        response.raise_for_status()

        return response.json()