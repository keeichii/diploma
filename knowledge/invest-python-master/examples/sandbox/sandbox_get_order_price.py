import os

from _decimal import Decimal

from t_tech.invest import utils
from t_tech.invest.sandbox.client import SandboxClient
from t_tech.invest.schemas import (
    GetOrderPriceRequest,
    GetOrderPriceResponse,
    OrderDirection,
)


def main():
    token = os.environ["INVEST_TOKEN"]

    with SandboxClient(token) as client:
        accounts = client.users.get_accounts()
        account_id = accounts.accounts[0].id
        response = get_order_price(client, account_id, "BBG004730ZJ9", 105)
        print(utils.money_to_decimal(response.total_order_amount))


def get_order_price(
    sandbox_service, account_id, instrument_id, price
) -> GetOrderPriceResponse:
    return sandbox_service.sandbox.get_sandbox_order_price(
        request=GetOrderPriceRequest(
            account_id=account_id,
            instrument_id=instrument_id,
            direction=OrderDirection.ORDER_DIRECTION_BUY,
            quantity=1,
            price=utils.decimal_to_quotation(Decimal(price)),
        )
    )


if __name__ == "__main__":
    main()
