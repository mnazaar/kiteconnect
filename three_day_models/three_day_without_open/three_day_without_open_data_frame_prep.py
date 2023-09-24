import datetime
import numpy as np
import pytz
import pandas as pd
from pandas import DataFrame


def prepare_history(add_today_this):
    all_history = pd.read_csv("./data/all_history.csv")
    all_history['date'] = pd.to_datetime(all_history['date'])
    all_history['instrument_code'] = all_history['instrument_code'].astype(str)
    all_history['date'] = pd.to_datetime(all_history['date'])

    if add_today_this:
        all_history_latest = all_history[all_history['date'] == max(all_history['date'])]
        custom_timezone = datetime.timezone(
            datetime.timedelta(hours=5, minutes=30))
        current_time = datetime.datetime.now(tz=custom_timezone)
        current_time = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        all_history_latest['date'] = current_time
        all_history_latest['open'] = all_history_latest['close']
        all_history_latest['high'] = 0
        all_history_latest['low'] = 0
        all_history_latest['close'] = 0
        all_history_latest['volume'] = 0
        all_history = pd.concat([all_history, all_history_latest], ignore_index=True)

    sorted_history_this = all_history.sort_values(by=['instrument_code', 'date'], ascending=[True, True])
    return sorted_history_this


def prepare_model_data_frame(historical_data, instrument_code_this):
    num_rows = len(historical_data)
    processed_data_frame = pd.DataFrame(
        columns=['date', 'instrument_code', 'instrument_token', 'open', 'high', 'low', 'close', 'volume', 'openD1',
                 'highD1', 'lowD1', 'closeD1', 'volumeD1', 'openD2', 'highD2', 'lowD2', 'closeD2', 'volumeD2', 'openD3',
                 'highD3', 'lowD3', 'closeD3', 'volumeD3',
                 ])
    for i in range(num_rows):
        print(f"Processing {instrument_code_this} - Row {i} of Total Rows {num_rows}")

        if i > 2:
            new_row = {'date': historical_data.iloc[i]['date'],
                       'instrument_code': historical_data.iloc[i]['instrument_code'],
                       'instrument_token': historical_data.iloc[i]['instrument_token'],
                       'open': historical_data.iloc[i]['open'],
                       'high': historical_data.iloc[i]['high'],
                       'low': historical_data.iloc[i]['low'],
                       'close': historical_data.iloc[i]['close'],
                       'volume': historical_data.iloc[i]['volume'],
                       'openD1': historical_data.iloc[i - 1]['open'],
                       'highD1': historical_data.iloc[i - 1]['high'],
                       'lowD1': historical_data.iloc[i - 1]['low'],
                       'closeD1': historical_data.iloc[i - 1]['close'],
                       'volumeD1': historical_data.iloc[i - 1]['volume'],
                       'openD2': historical_data.iloc[i - 2]['open'],
                       'highD2': historical_data.iloc[i - 2]['high'],
                       'lowD2': historical_data.iloc[i - 2]['low'],
                       'closeD2': historical_data.iloc[i - 2]['close'],
                       'volumeD2': historical_data.iloc[i - 2]['volume'],
                       'openD3': historical_data.iloc[i - 3]['open'],
                       'highD3': historical_data.iloc[i - 3]['high'],
                       'lowD3': historical_data.iloc[i - 3]['low'],
                       'closeD3': historical_data.iloc[i - 3]['close'],
                       'volumeD3': historical_data.iloc[i - 3]['volume'],
                       }
            processed_data_frame.loc[len(processed_data_frame)] = new_row
    return processed_data_frame


def populate_normalized_columns(processed_data_frame_this, column_code):
    if column_code == '':
        column_code_pre = column_code
    else:
        column_code_pre = column_code + '_'

    processed_data_frame_this[f'open_{column_code_pre}normalized'] = processed_data_frame_this[f'open{column_code}'] / \
                                                                     processed_data_frame_this['close_factor']

    processed_data_frame_this[f'high_{column_code_pre}normalized'] = processed_data_frame_this[f'high{column_code}'] / \
                                                                     processed_data_frame_this['close_factor']
    processed_data_frame_this[f'low_{column_code_pre}normalized'] = processed_data_frame_this[f'low{column_code}'] / \
                                                                    processed_data_frame_this['close_factor']
    processed_data_frame_this[f'close_{column_code_pre}normalized'] = processed_data_frame_this[f'close{column_code}'] \
                                                                      / processed_data_frame_this['close_factor']
    processed_data_frame_this[f'volume_{column_code_pre}normalized'] = processed_data_frame_this[
                                                                           f'volume{column_code}'] / \
                                                                       processed_data_frame_this['volume_factor']

    return processed_data_frame_this


def add_factors(basic_model_df_this):
    basic_model_df_this["close_factor"] = basic_model_df_this.apply(lambda row: row["closeD1"] / 100, axis=1)
    basic_model_df_this["volume_factor"] = basic_model_df_this.apply(lambda row: row["volumeD1"] / 100, axis=1)
    basic_model_df_this["open_factor"] = basic_model_df_this.apply(lambda row: row["open"] / 100, axis=1)
    return basic_model_df_this


def get_data_frame_for_training(add_today_this):
    sorted_history = prepare_history(add_today_this)
    instruments = sorted_history["instrument_code"].unique()
    final_df = pd.DataFrame()
    for instrument_code in instruments:
        filtered_history = sorted_history[sorted_history['instrument_code'] == instrument_code]
        instrument_data_frame: DataFrame = get_data_frame_for_instrument(instrument_code, filtered_history)
        final_df = pd.concat([final_df, instrument_data_frame], ignore_index=True)
    return final_df


def get_data_frame_for_instrument(instrument_code, filtered_history_this):
    basic_model_df = prepare_model_data_frame(filtered_history_this, instrument_code)
    basic_model_df_with_factors = add_factors(basic_model_df)
    model_df_with_normalized_values = populate_normalized_columns(basic_model_df_with_factors, '')
    model_df_with_normalized_values = populate_normalized_columns(model_df_with_normalized_values, 'D1')
    model_df_with_normalized_values = populate_normalized_columns(model_df_with_normalized_values, 'D2')
    model_df_with_normalized_values = populate_normalized_columns(model_df_with_normalized_values, 'D3')
    return model_df_with_normalized_values
