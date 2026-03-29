import os

from t_tech.invest import Client, InstrumentIdType, InstrumentRequest

TOKEN = os.environ["INVEST_TOKEN"]


def main():
    with Client(TOKEN) as client:
        dfa = client.instruments.dfa_by(
            request=InstrumentRequest(
                id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_POSITION_UID,
                id="ce604b33-70c7-4609-9f42-075dbd9fe278",
            )
        )
        print(dfa)


if __name__ == "__main__":
    main()
