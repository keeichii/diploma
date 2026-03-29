import os

from t_tech.invest import Client
from t_tech.invest.schemas import InstrumentsRequest, InstrumentStatus


def main():
    token = os.environ["INVEST_TOKEN"]

    with Client(token) as client:
        r = client.instruments.structured_notes(
            request=InstrumentsRequest(
                instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL
            )
        )
        for note in r.instruments:
            print(note)


if __name__ == "__main__":
    main()
