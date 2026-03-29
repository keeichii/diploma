import os
from datetime import timedelta

from t_tech.invest import CandleInterval, Client
from t_tech.invest.schemas import CandleSource
from t_tech.invest.utils import now

TOKEN = os.environ["INVEST_TOKEN"]


def main():
    with Client(TOKEN) as client:
        for candle in client.get_all_candles(
            instrument_id="BBG004730N88",
            from_=now() - timedelta(days=365),
            interval=CandleInterval.CANDLE_INTERVAL_HOUR,
            candle_source_type=CandleSource.CANDLE_SOURCE_UNSPECIFIED,
        ):
            print(candle)

    return 0


if __name__ == "__main__":
    main()
