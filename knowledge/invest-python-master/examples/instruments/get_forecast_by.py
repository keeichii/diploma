import os
from datetime import timedelta

from t_tech.invest import Client
from t_tech.invest.schemas import (
    GetAssetReportsRequest,
    GetConsensusForecastsRequest,
    GetForecastRequest,
    InstrumentIdType,
    Page,
)
from t_tech.invest.utils import now

TOKEN = os.environ["INVEST_TOKEN"]


def main():
    with Client(TOKEN) as client:
        instrument = client.instruments.find_instrument(
            query="Сбер Банк - привилегированные акции"
        ).instruments[0]
        request = GetForecastRequest(instrument_id=instrument.uid)
        response = client.instruments.get_forecast_by(request=request)
        print(instrument.name, response.consensus.recommendation.name)


if __name__ == "__main__":
    main()
