import asyncio
import os

from _decimal import Decimal

from t_tech.invest import utils
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.schemas import (
    GetOrderPriceRequest,
    GetOrderPriceResponse,
    OrderDirection,
)


async def main():
    token = os.environ["INVEST_TOKEN"]

    async with AsyncSandboxClient(token) as client:
        accounts = await client.users.get_accounts()
        account_id = accounts.accounts[0].id
        response = await get_async_order_price(client, account_id, 105)
        print(utils.money_to_decimal(response.total_order_amount))


async def get_async_order_price(
    sandbox_service, account_id, price
) -> GetOrderPriceResponse:
    return await sandbox_service.sandbox.get_sandbox_order_price(
        request=GetOrderPriceRequest(
            account_id=account_id,
            instrument_id="BBG004730ZJ9",
            direction=OrderDirection.ORDER_DIRECTION_BUY,
            quantity=1,
            price=utils.decimal_to_quotation(Decimal(price)),
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
