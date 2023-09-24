import pandas as pd
from keras import layers
from keras.models import Sequential
from keras.src.callbacks import EarlyStopping
from keras.src.losses import MeanSquaredError
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error

from three_day_models.three_day_with_open.three_day_with_open_data_frame_prep import get_data_frame_for_training

input_columns = ['open', 'open_D1_normalized', 'high_D1_normalized', 'low_D1_normalized', 'close_D1_normalized',
                 'volume_D1_normalized', 'open_D2_normalized', 'high_D2_normalized', 'low_D2_normalized',
                 'close_D2_normalized', 'volume_D2_normalized', 'open_D3_normalized', 'high_D3_normalized',
                 'low_D3_normalized', 'close_D3_normalized', 'volume_D3_normalized']
output_columns = ['high_normalized']


def get_sorted_dataframe(add_today_this):
    data_frame_this = get_data_frame_for_training(add_today_this)
    data_frame_this = data_frame_this.sort_values(by=['date', 'instrument_code'])
    return data_frame_this


def prepare_model():
    model_this = Sequential()
    model_this.add(layers.Input(shape=(16,)))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(64, activation='relu'))
    model_this.add(layers.Dense(1, activation='linear'))
    model_this.compile(optimizer='adam', loss=MeanSquaredError())
    return model_this


def prepare_train_test_split(data_frame_this):
    data_frame_this['date'] = pd.to_datetime(data_frame_this['date'])
    data_frame_this['instrument_code'] = data_frame_this['instrument_code'].astype(str)
    data_frame_this = data_frame_this.sort_values(by=['date', 'instrument_code'], ascending=[True, True])
    data_frame_this.reset_index(drop=True, inplace=True)
    train_df_this = data_frame_this[~ (data_frame_this['date'] == max(data_frame_this['date']))]
    test_df_this = data_frame_this[data_frame_this['date'] == max(data_frame_this['date'])]
    return train_df_this, test_df_this, train_df_this[input_columns], train_df_this[output_columns], \
        test_df_this[input_columns], test_df_this[output_columns]


def get_prediction():
    early_stopping = EarlyStopping(monitor='loss', patience=20)
    model.fit(train_x, train_y, epochs=1000, batch_size=32, callbacks=[early_stopping])
    prediction_y_this = model.predict(test_x)
    prediction_y_this = pd.DataFrame(prediction_y_this)
    return prediction_y_this


def add_columns_to_final_output(test_y_this, test_df_this):
    test_y_this.reset_index(drop=True, inplace=True)
    test_df_this.reset_index(drop=True, inplace=True)
    test_y_this.insert(0, 'date', test_df['date'])
    test_y_this.insert(1, 'instrument_code', test_df['instrument_code'])
    test_y_this.insert(3, 'high_normalized_pred', prediction_y[0])
    test_y_this.insert(4, 'open', test_df['open'])
    test_y_this.insert(5, 'close', test_df['close'])
    test_y_this.insert(6, 'high', test_df['high'])
    test_y_this.insert(7, 'high_predicted', test_y['high_normalized_pred'] * test_df['open_factor'])
    return test_y_this, test_df_this


def predict_changes(add_today_this):
    global model, train_df, test_df, train_x, train_y, test_x, test_y, prediction_y
    data_frame = get_sorted_dataframe(add_today_this)
    model = prepare_model()
    train_df, test_df, train_x, train_y, test_x, test_y = prepare_train_test_split(data_frame)
    prediction_y = get_prediction()
    test_y, test_df = add_columns_to_final_output(test_y, test_df)
    mse = mean_squared_error(test_y['high_normalized'], test_y['high_normalized_pred'])
    mae = mean_absolute_error(test_y['high_normalized'], test_y['high_normalized_pred'])
    r2 = r2_score(test_y['high_normalized'], test_y['high_normalized_pred'])
    model_name = "Keras 3 Day Without open"
    print(data_frame.size)
    print(f"{model_name} Mean Absolute Error (MAE): {mae}")
    print(f"{model_name} Mean Squared Error (MSE): {mse}")
    print(f"{model_name} R^2 (Coefficient of Determination): {r2}")
    return test_y
