import os

from t_tech.invest import Client

TOKEN = os.environ["INVEST_TOKEN"]


def main():
    with Client(TOKEN) as client:
        r = client.instruments.dfas()
        for dfa in r.instruments:
            print(dfa)


if __name__ == "__main__":
    main()
