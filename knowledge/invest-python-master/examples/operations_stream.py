import os

from t_tech.invest import Client
from t_tech.invest.schemas import OperationsStreamRequest, PingDelaySettings

TOKEN = os.environ["INVEST_TOKEN"]


def main():
    with Client(TOKEN) as client:
        accounts = client.users.get_accounts().accounts
        accounts = [i.id for i in accounts]
        print(f"Subscribe for operations on accounts: {accounts}")
        for operation in client.operations_stream.operations_stream(
            OperationsStreamRequest(
                accounts=accounts,
                ping_settings=PingDelaySettings(
                    ping_delay_ms=5000,
                ),
            )
        ):
            print(operation)


if __name__ == "__main__":
    main()
