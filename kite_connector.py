import os

from kiteconnect import KiteConnect
import pandas as pd

KITE_API_SECRET = os.environ.get('KITE_API_SECRET')
KITE_API_KEY = os.environ.get('KITE_API_KEY')


class KiteConnector:
    _instance = None

    def __init__(self):
        self.df_instruments = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KiteConnector, cls).__new__(cls)
            cls._instance.connector = KiteConnect(api_key=KITE_API_KEY)
            cls._instance.re_use_key = False
        else:
            cls._instance.re_use_key = True
        return cls._instance

    def set_access_token(self, request_token):
        data = self.connector.generate_session(request_token, api_secret=KITE_API_SECRET)
        self.connector.set_access_token(data["access_token"])

    def get_instrument_details(self, instrument_code, exchange_code):
        if self.df_instruments is None:
            instruments = self.connector.instruments()
            self.df_instruments = pd.DataFrame(instruments)
        filtered_df = self.df_instruments[
            (self.df_instruments["tradingsymbol"] == instrument_code) & (self.df_instruments["exchange"] == exchange_code) & (
                    self.df_instruments["instrument_type"] == "EQ")]
        return filtered_df

    def get_instrument_history(self, instrument_code, exchange_code, from_date, to_date):
        filtered_df = self.get_instrument_details(instrument_code, exchange_code)

        result = self.connector.historical_data(instrument_token=filtered_df.iloc[0, 0], from_date=from_date,
                                                to_date=to_date, interval="day")
        df_historical_data =  pd.DataFrame(result)
        df_historical_data.insert(1, 'instrument_code', instrument_code)
        df_historical_data.insert(2, 'instrument_token', filtered_df.iloc[0, 0])
        return df_historical_data


    def get_latest_info(self, instruments_array):

        result = self.connector.ltp(instruments_array)

        return result

    def place_order_real(self, dataframe_row, buy_sell):
        if buy_sell == self.connector.TRANSACTION_TYPE_SELL:
            quantity = int(dataframe_row['quantity'])
        else:
            quantity = int(dataframe_row['fund_allotted']/dataframe_row['ltp'])
        print(f"Transacting - {dataframe_row.to_string(index=False)}")
        self.connector.place_order(variety=self.connector.VARIETY_REGULAR, exchange=dataframe_row['exchange'],
                                   tradingsymbol=dataframe_row['instrument_symbol'],
                                   transaction_type=buy_sell, quantity=quantity,
                                   product=self.connector.PRODUCT_CNC,
                                   order_type=self.connector.ORDER_TYPE_MARKET)

    def place_order_test(self, dataframe_row, buy_sell):
        if buy_sell == self.connector.TRANSACTION_TYPE_SELL:
            quantity = int(dataframe_row['quantity'])
        else:
            quantity = int(dataframe_row['fund_allotted']/dataframe_row['ltp'])

        print(f"variety={self.connector.VARIETY_REGULAR}, exchange={dataframe_row['exchange']},"
              f"tradingsymbol={dataframe_row['instrument_symbol']},  transaction_type={buy_sell}, "
              f"quantity={quantity},                                   "
              f"product={self.connector.PRODUCT_CNC},                                "
              f"order_type={self.connector.ORDER_TYPE_MARKET}")

