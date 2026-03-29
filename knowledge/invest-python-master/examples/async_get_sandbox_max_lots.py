import asyncio
import os

from t_tech.invest import GetMaxLotsRequest
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.sandbox.client import SandboxClient

TOKEN = os.environ["INVEST_SANDBOX_TOKEN"]


async def main():
    async with AsyncSandboxClient(TOKEN) as client:
        account_id = (await client.users.get_accounts()).accounts[0].id
        request = GetMaxLotsRequest(
            account_id=account_id,
            instrument_id="BBG004730N88",
        )
        print(await client.sandbox.get_sandbox_max_lots(request=request))


if __name__ == "__main__":
    asyncio.run(main())
