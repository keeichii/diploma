import os
from datetime import timedelta

from t_tech.invest import Client
from t_tech.invest.schemas import (
    GetAssetReportsRequest,
    GetConsensusForecastsRequest,
    InstrumentIdType,
    Page,
)
from t_tech.invest.utils import now

TOKEN = os.environ["INVEST_TOKEN"]


def main():
    with Client(TOKEN) as client:
        request = GetConsensusForecastsRequest(
            paging=Page(page_number=0, limit=2),
        )
        response = client.instruments.get_consensus_forecasts(request=request)
        print(response.page)
        for forecast in response.items:
            print(forecast.uid, forecast.consensus.name)


if __name__ == "__main__":
    main()
