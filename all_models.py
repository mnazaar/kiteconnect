import datetime
import time

from three_day_models import three_day_without_open, three_day_with_open
from three_day_models.three_day_without_open.three_day_without_open_mlp_model import \
    predict_changes as three_day_mlp_without_open_predict_changes
from three_day_models.three_day_without_open.three_day_without_open_keras_model import \
    predict_changes as three_day_keras_without_open_predict_changes
from three_day_models.three_day_with_open.three_day_with_open_mlp_model import \
    predict_changes as three_day_mlp_with_open_predict_changes
from three_day_models.three_day_with_open.three_day_with_open_keras_model import \
    predict_changes as three_day_keras_with_open_predict_changes

from three_day_models.ten_day_without_open.ten_day_without_open_mlp_model import \
    predict_changes as ten_day_mlp_without_open_predict_changes

from three_day_models.ten_day_without_open.ten_day_without_open_keras_model import \
    predict_changes as ten_day_keras_without_open_predict_changes

add_today_this = False

model1_start = time.time()
mlp_result = three_day_mlp_without_open_predict_changes(add_today_this)
model1_end = time.time()


model2_start = time.time()
keras_result = three_day_keras_without_open_predict_changes(add_today_this)
model2_end = time.time()

model3_start = time.time()
mlp_with_open_result = three_day_mlp_with_open_predict_changes(add_today_this)
model3_end = time.time()

model4_start = time.time()
keras_with_open_result = three_day_keras_with_open_predict_changes(add_today_this)
model4_end = time.time()

model5_start = time.time()
mlp10_without_open_result = ten_day_mlp_without_open_predict_changes(add_today_this)
model5_end = time.time()


model6_start = time.time()
keras10_without_open_result = ten_day_keras_without_open_predict_changes(add_today_this)
model6_end = time.time()

mlp_result["high_normalized_orig"] = mlp_result["high_normalized"]
mlp_result["high_normalized_mlp_without_open"] = mlp_result["high_normalized_pred"]
mlp_result["high_normalized_mlp10_without_open"] = mlp10_without_open_result["high_normalized_pred"]
mlp_result["high_normalized_keras_without_open"] = keras_result["high_normalized_pred"]
mlp_result["high_normalized_keras10_without_open"] = keras10_without_open_result["high_normalized_pred"]

mlp_result["high_normalized_wiht_open_orig"] = mlp_with_open_result["high_normalized"]
mlp_result["high_normalized_mlp_with_open"] = mlp_with_open_result["high_normalized_pred"]
mlp_result["high_normalized_keras_with_open"] = keras_with_open_result["high_normalized_pred"]

mlp_result["high_orig"] = mlp_result["high"]
mlp_result["high_mlp_without_open"] = mlp_result["high_predicted"]
mlp_result["high_mlp10_without_open"] = mlp10_without_open_result["high_predicted"]
mlp_result["high_keras_without_open"] = keras_result["high_predicted"]
mlp_result["high_keras10_without_open"] = keras10_without_open_result["high_predicted"]
mlp_result["high_mlp_with_open"] = mlp_with_open_result["high_predicted"]
mlp_result["high_keras_with_open"] = keras_with_open_result["high_predicted"]

mlp_result.to_csv(f"./data/results/results{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M_%S')}.csv")


print(f"model 1 time {str(model1_end-model1_start)}")
print(f"model 2 time {str(model2_end-model2_start)}")
print(f"model 3 time {str(model3_end-model3_start)}")
print(f"model 4 time {str(model4_end-model4_start)}")
print(f"model 5 time {str(model5_end-model5_start)}")
print(f"model 6 time {str(model6_end-model6_start)}")



