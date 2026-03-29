import asyncio
import os

from _decimal import Decimal

from t_tech.invest import AccountType, AsyncClient
from t_tech.invest.schemas import PayInRequest
from t_tech.invest.utils import decimal_to_money


async def main():
    token = os.environ["INVEST_TOKEN"]

    async with AsyncClient(token) as client:
        accounts = [
            i
            for i in (await client.users.get_accounts()).accounts
            if i.type == AccountType.ACCOUNT_TYPE_TINKOFF
        ]

        bank_accounts = await client.users.get_bank_accounts()

        if len(accounts) < 1 or len(bank_accounts.bank_accounts) < 1:
            print("Недостаточно счетов для демонстрации")
            return

        from_account_id = bank_accounts.bank_accounts[0].id
        to_account_id = accounts[0].id

        await client.users.pay_in(
            PayInRequest(
                from_account_id=from_account_id,
                to_account_id=to_account_id,
                amount=decimal_to_money(Decimal(1), "rub"),
            )
        )
        print("Пополнение выполнено")


if __name__ == "__main__":
    asyncio.run(main())
