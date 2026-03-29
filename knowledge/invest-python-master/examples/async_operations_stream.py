import asyncio
import os

from t_tech.invest import AsyncClient
from t_tech.invest.schemas import OperationsStreamRequest, PingDelaySettings

TOKEN = os.environ["INVEST_TOKEN"]


async def main():
    async with AsyncClient(TOKEN) as client:
        accounts = await client.users.get_accounts()
        accounts = [i.id for i in accounts.accounts]
        print(f"Subscribe for operations on accounts: {accounts}")
        async for operation in client.operations_stream.operations_stream(
            OperationsStreamRequest(
                accounts=accounts,
                ping_settings=PingDelaySettings(
                    ping_delay_ms=10_000,
                ),
            )
        ):
            print(operation)


if __name__ == "__main__":
    asyncio.run(main())
