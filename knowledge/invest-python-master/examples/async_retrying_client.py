import asyncio
import logging
import os
from datetime import timedelta

from t_tech.invest import CandleInterval
from t_tech.invest.retrying.aio.client import AsyncRetryingClient
from t_tech.invest.retrying.settings import RetryClientSettings
from t_tech.invest.utils import now

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.DEBUG)

TOKEN = os.environ["INVEST_TOKEN"]

retry_settings = RetryClientSettings(use_retry=True, max_retry_attempt=2)


async def main():
    async with AsyncRetryingClient(TOKEN, settings=retry_settings) as client:
        async for candle in client.get_all_candles(
            figi="BBG000B9XRY4",
            from_=now() - timedelta(days=301),
            interval=CandleInterval.CANDLE_INTERVAL_1_MIN,
        ):
            print(candle)


if __name__ == "__main__":
    asyncio.run(main())
