import os
import uuid

from _decimal import Decimal

from t_tech.invest import AccountType, Client
from t_tech.invest.schemas import CurrencyTransferRequest
from t_tech.invest.utils import decimal_to_money


def main():
    token = os.environ["INVEST_TOKEN"]

    with Client(token) as client:
        accounts = [
            i
            for i in client.users.get_accounts().accounts
            if i.type == AccountType.ACCOUNT_TYPE_TINKOFF
        ]

        if len(accounts) < 2:
            print("Недостаточно счетов для демонстрации")
            return

        from_account_id = accounts[0].id
        to_account_id = accounts[1].id

        client.users.currency_transfer(
            CurrencyTransferRequest(
                from_account_id=from_account_id,
                to_account_id=to_account_id,
                amount=decimal_to_money(Decimal(1), "rub"),
                transaction_id=str(uuid.uuid4()),
            )
        )
        print("Перевод выполнен")


if __name__ == "__main__":
    main()
