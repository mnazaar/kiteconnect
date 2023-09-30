import time
import traceback

import flask
import urllib.request

import pandas as pd
from flask import Flask, render_template, redirect, request, url_for
from kiteconnect import KiteConnect
from datetime import datetime

from kiteconnect.exceptions import InputException

import kite_connector

app = Flask(__name__)
app.secret_key = 'FISIntelligenceSystemSecretKey'


@app.route("/naztrade/redirect", methods=["GET"])
def redirect_trade():
    request_token = request.values.get('request_token')
    kite_connector_this = kite_connector.KiteConnector()
    kite_connector_this.set_access_token(request_token)
    return render_template("prices_ajax.html", request_token=request_token, values_html="Welcome")


@app.route("/naztrade/save_instrument_details", methods=["GET"])
def save_instrument_details():
    request_token = request.values.get('request_token')
    instrument_code = request.values.get('instrument_code')
    from_date = request.values.get('from_date')
    to_date = request.values.get('to_date')
    exchange_code = request.values.get('exchange_code')

    if instrument_code == "GETALL":
        instruments = get_instruments_as_list()
        for instrument in instruments:
            print(instrument)
            results = add_to_csv(instrument, exchange_code, from_date, to_date)
    else:
        results = add_to_csv(instrument_code, exchange_code, from_date, to_date)

    return render_template("kite_landing.html", request_token=request_token,
                           values_html=results.to_html())


def add_to_csv(instrument_code, exchange_code, from_date, to_date):
    try:
        file_found = True
        all_history = pd.read_csv("./data/all_history.csv")
    except FileNotFoundError:
        file_found = False

    kite_connector_this = kite_connector.KiteConnector()
    results = kite_connector_this.get_instrument_history(instrument_code, exchange_code, from_date,
                                                         to_date)
    if file_found:
        all_history['date'] = pd.to_datetime(all_history['date'])
        filtered_history = all_history[~(all_history['date'].isin(results['date']) & all_history['instrument_token'].
                                         isin(results['instrument_token']))]
        filtered_history.to_csv("./data/all_history.csv", header=True, index=False)

        results.to_csv("./data/all_history.csv", mode='a', header=False, index=False)
    else:
        results.to_csv("./data/all_history.csv", header=True, index=False)
    return results


@app.route("/naztrade/landing", methods=["GET"])
def show_login_page():
    kite_connector_this = kite_connector.KiteConnector()

    if kite_connector_this.re_use_key:
        return redirect(url_for('redirect_trade'))
    else:
        return redirect(kite_connector_this.connector.login_url())


@app.route("/naztrade/analyze_tokens", methods=["GET"])
def analyze_tokens():
    all_instruments = pd.read_csv("./data/all_instruments.csv")
    connector = kite_connector.KiteConnector()
    for item in all_instruments.iterrows():
        result = connector.get_instrument_details(item[1]["INSTRUMENT_CODE"], item[1]["EXCHANGE"])
        if len(result) != 1:
            print(str(len(result)) + item[1]["INSTRUMENT_CODE"])
    return "hello"


def get_ltp_from_ltp_results_real(row, results_ltp):
    return results_ltp[f"{row['exchange']}:{row['instrument_symbol']}"]["last_price"]


def get_ltp_from_ltp_results_test(row, results_ltp):
    return row['ltp'] + row['ltp'] * 0.0025


def get_revised_stoploss(row):
    ltp = row['ltp']
    current_gap_percentage = row['sl_gap_percent']
    current_gap = round(ltp * current_gap_percentage / 100, 2)

    current_stop_loss = row['revised_stoploss']

    protect_after = row['protect_after']

    new_gap = ltp - current_stop_loss

    if ltp > protect_after and new_gap > current_gap:
        current_stop_loss = ltp - current_gap

    return current_stop_loss


def get_revised_target(row):
    ltp = row['ltp']
    current_gap_percent = row['tgt_gap_percent']
    current_gap = round(ltp * current_gap_percent / 100, 2)

    current_target = row['revised_target']
    protect_after = row['protect_after']

    new_gap = current_target - ltp

    if ltp > protect_after and new_gap < current_gap:
        current_target = current_target + current_gap - new_gap

    return current_target


def get_buy_quantity(row):
    quantity = row['quantity']
    if row['status'] == 'Buy':
        buy_bo_trigger = row['buy_bo_trigger']
        buy_sr_trigger = row['buy_sr_trigger']
        distance_to_bo_trigger = buy_bo_trigger - row['ltp']
        distance_to_sr_trigger = row['ltp'] - buy_sr_trigger
        distance_to_nearest_trigger = min(abs(distance_to_bo_trigger), abs(distance_to_sr_trigger))
        if distance_to_nearest_trigger == distance_to_bo_trigger:
            if buy_bo_trigger == 0:
                quantity = int(row['fund_allotted'] / buy_sr_trigger)
            else:
                quantity = int(row['fund_allotted'] / buy_bo_trigger)
        else:
            quantity = int(row['fund_allotted'] / buy_sr_trigger)

    return quantity


def get_distance_percentage(row):
    percentage = 0
    if row['status'] == 'Holding':
        current_target = row['revised_target']
        current_stoploss = row['revised_stoploss']
        distance_to_target = current_target - row['ltp']
        distance_to_stop_loss = current_stoploss - row['ltp']

        if abs(distance_to_stop_loss) < distance_to_target:
            percentage = round(100 * distance_to_stop_loss / row['ltp'], 2)
        else:
            percentage = round(100 * distance_to_target / row['ltp'], 2)
    else:
        buy_bo_trigger = row['buy_bo_trigger']
        buy_sr_trigger = row['buy_sr_trigger']
        distance_to_bo_trigger = buy_bo_trigger - row['ltp']
        distance_to_sr_trigger = row['ltp'] - buy_sr_trigger

        distance_to_nearest_trigger = min(abs(distance_to_bo_trigger), abs(distance_to_sr_trigger))
        percentage = round(100 * distance_to_nearest_trigger / row['ltp'], 2)

    return percentage


def get_row_colour(row):
    current_target = row['revised_target']
    original_target = row['target_original']
    current_stoploss = row['revised_stoploss']
    original_stoploss = row['stoploss_original']
    ltp = row['ltp']
    cost = row['cost']

    profit = ltp - cost

    color = '#FFFFFF'
    if row['status'] != 'Holding':
        color = '#AB9EFF'  ##Blue
    elif ltp < current_stoploss:
        color = '#C80372'  ##Red
    elif ltp > original_target:
        color = '#099346'  ##green
    elif profit >= 0:
        color = '#B7D793'  ##light green
    else:
        color = '#E7A8B2'  ## pink

    return color


def place_order(row):
    response_status = row['status']
    response_bo_trigger = row['buy_bo_trigger']
    response_sr_trigger = row['buy_sr_trigger']
    if row['status'] == 'Holding' and row['ltp'] < row['revised_stoploss']:
        kite_connector_this = kite_connector.KiteConnector()
        kite_connector_this.place_order_real(row, kite_connector_this.connector.TRANSACTION_TYPE_SELL)
        response_status = 'Sold'
        response_bo_trigger = 0
        response_sr_trigger = 0
    elif row['status'] == 'Buy' and row['buy_bo_trigger'] > 0 and row['ltp'] > row['buy_bo_trigger']:
        kite_connector_this = kite_connector.KiteConnector()
        kite_connector_this.place_order_real(row, kite_connector_this.connector.TRANSACTION_TYPE_BUY)
        response_status = 'Holding'
        response_bo_trigger = 0
        response_sr_trigger = 0
    elif row['status'] == 'Buy' and row['buy_sr_trigger'] > 0 and row['ltp'] < row['buy_sr_trigger']:
        ltp = row['ltp']
        stop_loss = row['revised_stoploss']
        cancel_at = round_to_nearest_0_05(stop_loss - (stop_loss * row['sl_buy_tolerance_percent'] / 100))

        if ltp < cancel_at:
            response_status = 'Cancelled'
            response_bo_trigger = 0
            response_sr_trigger = 0
        else:
            response_status = 'Buy'
            new_buy_trigger = ltp + (ltp * row['rev_buy_percent'] / 100)
            if row['buy_bo_trigger'] == 0 or new_buy_trigger < row['buy_bo_trigger']:
                response_bo_trigger = new_buy_trigger
            else:
                response_bo_trigger = row['buy_bo_trigger']
            response_sr_trigger = row['buy_sr_trigger']

    return response_status, round_to_nearest_0_05(response_bo_trigger), round_to_nearest_0_05(response_sr_trigger)


def round_to_nearest_0_05(number):
    return round(number * 20) / 20


@app.route("/naztrade/refresh_prices")
def refresh_prices():
    all_prices = pd.read_csv("./data/monitoring.csv")
    formatted_array = all_prices.apply(lambda row: f"{row['exchange']}:{row['instrument_symbol']}", axis=1)
    all_prices["fetch_status"] = "success"

    all_prices["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    kite_connector_this = kite_connector.KiteConnector()
    try:
        ltp_results = kite_connector_this.get_latest_info(formatted_array.to_list())
        round_amount_columns(all_prices)

        all_prices["ltp"] = round(all_prices.apply(get_ltp_from_ltp_results_real, axis=1, results_ltp=ltp_results), 2)
        all_prices["current_value"] = round(all_prices["ltp"] * all_prices["quantity"], 2)
        all_prices["investment_amount"] = round(all_prices["cost"] * all_prices["quantity"], 2)
        all_prices["profit"] = round(all_prices["current_value"] - all_prices["investment_amount"], 2)
        all_prices["profit_percent"] = all_prices["profit"] * 100 / all_prices["investment_amount"]
        all_prices["count"] = all_prices["count"] + 1
        all_prices['revised_stoploss'] = all_prices.apply(get_revised_stoploss, axis=1)
        all_prices['revised_target'] = all_prices.apply(get_revised_target, axis=1)
        all_prices['row_colour'] = all_prices.apply(get_row_colour, axis=1)
        all_prices['distance'] = all_prices.apply(get_distance_percentage, axis=1)
        all_prices['quantity'] = all_prices.apply(get_buy_quantity, axis=1)

        all_prices[['status', 'buy_bo_trigger', 'buy_sr_trigger']] = all_prices.apply(place_order, axis=1,
                                                                                      result_type='expand')
        all_prices.to_csv("./data/monitoring.csv", header=True, index=False)

        return render_template_dataframes(all_prices)

    except Exception as e:
        all_prices["fetch_status"] = "failed"
        all_prices['row_colour'] = "#C80372"
        traceback.print_exc()
        return render_template_dataframes(all_prices)


def add_cancel_at(row):
    return round_to_nearest_0_05(
        row['revised_stoploss'] - (row['revised_stoploss'] * row['sl_buy_tolerance_percent'] / 100))


def render_template_dataframes(all_prices):
    all_prices['cancel_below'] = all_prices.apply(add_cancel_at, axis=1)

    all_monitoring = all_prices[all_prices["status"] == "Holding"]
    all_monitoring = all_monitoring.sort_values(by="distance", ascending=False)
    all_sold = all_prices[(all_prices['status'] == 'Sold') | (all_prices['status'] == 'Cancelled') | (
            all_prices['status'] == 'Exited')]
    all_sold = all_sold.sort_values(by="distance", ascending=False)
    all_buy = all_prices[all_prices['status'] == 'Buy']

    all_buy = all_buy.sort_values(by="distance", ascending=False)
    all_buy_sorted_df = all_buy.iloc[all_buy['distance'].abs().argsort()]

    return render_template("prices_shares.html", monitorings=all_monitoring, buys=all_buy_sorted_df, solds=all_sold)


def round_amount_columns(all_prices):
    all_prices['ltp'] = all_prices['ltp'].astype(float).round(decimals=2)
    all_prices['cost'] = all_prices['cost'].astype(float).round(decimals=2)
    all_prices['stoploss_original'] = all_prices['stoploss_original'].astype(float).round(decimals=2)
    all_prices['target_original'] = all_prices['target_original'].astype(float).round(decimals=2)
    all_prices['revised_stoploss'] = all_prices['revised_stoploss'].astype(float).round(decimals=2)
    all_prices['revised_target'] = all_prices['revised_target'].astype(float).round(decimals=2)
    all_prices['sl_gap_percent'] = all_prices['sl_gap_percent'].astype(float).round(decimals=2)
    all_prices['tgt_gap_percent'] = all_prices['tgt_gap_percent'].astype(float).round(decimals=2)
    all_prices['protect_after'] = all_prices['protect_after'].astype(float).round(decimals=2)
    all_prices['quantity'] = all_prices['quantity'].astype(float).round(decimals=2)
    all_prices['investment_amount'] = all_prices['investment_amount'].astype(float).round(decimals=2)
    all_prices['current_value'] = all_prices['current_value'].astype(float).round(decimals=2)
    all_prices['profit'] = all_prices['profit'].astype(float).round(decimals=2)
    all_prices['profit_percent'] = all_prices['profit_percent'].astype(float).round(decimals=2)
    all_prices['distance'] = all_prices['distance'].astype(float).round(decimals=2)
    all_prices['buy_sr_trigger'] = all_prices['buy_sr_trigger'].astype(float).round(decimals=2)
    all_prices['buy_bo_trigger'] = all_prices['buy_bo_trigger'].astype(float).round(decimals=2)
    all_prices['count'] = all_prices['count'].astype(float).round(decimals=2)
    all_prices['sl_buy_tolerance_percent'] = all_prices['sl_buy_tolerance_percent'].astype(float).round(decimals=2)
    all_prices['rev_buy_percent'] = all_prices['rev_buy_percent'].astype(float).round(decimals=2)
    all_prices['fund_allotted'] = all_prices['fund_allotted'].astype(float).round(decimals=2)


@app.route("/naztrade/monitoring", methods=["GET"])
def monitoring():
    kite_connector_this = kite_connector.KiteConnector()

    if kite_connector_this.re_use_key:
        return render_template("prices_ajax.html")
    else:
        return redirect(kite_connector_this.connector.login_url())


def get_instruments_as_list():
    all_instruments = pd.read_csv("./data/all_instruments.csv")
    return all_instruments["INSTRUMENT_CODE"].tolist()


if __name__ == "__main__":
    all_prices_on_load = pd.read_csv("./data/monitoring.csv")
    all_prices_on_load['count'] = 0
    all_prices_on_load.to_csv("./data/monitoring.csv", header=True, index=False)

    app.run(debug=True)
