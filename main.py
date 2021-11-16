import binance.helpers
import ccxt
from binance.client import Client
from binance.enums import *
import pandas as pd
import sqlalchemy
import schedule
import time
from datetime import datetime
import warnings
from pprint import pprint
from binance.exceptions import *
from ccxt.base.errors import *
from requests.exceptions import *
import os


# create database in the first time that execute the code
def create_database():
    data = exchange.fetch_ohlcv(symbol + f'/{pair}', timeframe, limit=400)
    data_frame = pd.DataFrame(data[:-1], columns=['time', 'open', 'high', 'low', 'close', 'volume'])
    data_frame = data_frame.set_index(['time'])
    data_frame.index = pd.to_datetime(data_frame.index, unit='ms')
    data_frame = supertrends(data_frame)
    check = check_delta_time(data_frame)
    if check:
        data_frame.to_sql(f'{symbol}{pair}{timeframe}', engine, if_exists='replace')
        print('*' * 69)
        print(f'\nDATABASE: {symbol}{pair}{timeframe} HAD BEEN CREATED:')
        return True
    else:
        return False


def show_database(data_frame):
    print('Length of dataframe: ' + str(len(data_frame.index)) + f' P/L ratios: ({my_l_target}:1)')
    lower = binance.helpers.round_step_size(data_frame.lowerband[-1], tick_size / 10)
    upper = binance.helpers.round_step_size(data_frame.upperband[-1], tick_size / 10)
    if data_frame.in_uptrend[-1]:
        trend = 'Up trend'
        band = f'Lower Band: {lower}'
    elif not data_frame.in_uptrend[-1]:
        trend = 'Down trend'
        band = f'Upper Band: {upper}'
    else:
        trend = 'None'
        band = f'Lower Band: None'
    if round(data_frame.volume.mean()) >= data_frame.volume[-1]:
        mean_vol = 'Below mean'
    else:
        mean_vol = 'Beyond mean'
    if data_frame.close[-1] > data_frame.MA[-1]:
        ma_line = 'Close above MA'
    else:
        ma_line = 'Close below MA'
    ss = exchange.fetch_balance()[pair]['total'] * \
        float(leverage['leverage']) * set_amount / data_frame.close[-1]
    sz = binance.helpers.round_step_size(ss, step_size)
    state = f'Symbol: {symbol} | ' \
            f'Pair: {pair} | ' \
            f'Timeframe: {timeframe} | ' \
            f'Time: {datetime.today().strftime("%H:%M:%S")}\n' \
            f"Sizing: {sz} " \
            f"MIN({min_qty}) " \
            f'Step({step_size}) ' \
            f'Tick({tick_size})\n' \
            f'Data time: {data_frame.index[-2]} | ' \
            f'Close: {data_frame.close[-2]} | ' \
            f'MA{ma}: {binance.helpers.round_step_size(data_frame.MA[-2], tick_size / 10)}\n' \
            f'Data time: {data_frame.index[-1]} | ' \
            f'Close: {data_frame.close[-1]} | ' \
            f'MA{ma}: {binance.helpers.round_step_size(data_frame.MA[-1], tick_size / 10)}\n' \
            f'Trend: {trend} | {band} ({ma_line})\n' \
            f'Volume: {data_frame.volume[-1]} ({mean_vol}) ' \
            f'MEAN: {round(data_frame.volume.mean())}' \
            f' STD: ±{"{:.2f}".format(data_frame.volume.std())} {symbol}/{timeframe}'
    print(state + '\n' + str('-' * 69))


# fetch new dataframe for updating
def fetch_new(dt):
    while True:
        d_f = dt
        tf = timeframe
        data = exchange.fetch_ohlcv(symbol + f'/{pair}', tf, limit=3)
        data_frame = pd.DataFrame(data[:-1], columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        data_frame = data_frame.set_index(['time'])
        data_frame.index = pd.to_datetime(data_frame.index, unit='ms')
        cd1 = d_f.index[-1]
        cd2 = data_frame.index[-2]
        if cd2 == cd1:
            return data_frame.tail(1)
        time.sleep(0.1)


# keep update database
def update_database():
    data = dataframe()
    df_new = fetch_new(data)
    if df_new is None:
        return False
    else:
        data_frame = data.append(df_new)
        check = check_delta_time(data_frame)
        if check:
            data_frame = update_supertrends(data_frame)
            data_frame.tail(400).to_sql(f'{symbol}{pair}{timeframe}', engine, if_exists='replace')
            print('*' * 63)
            print(f'\nDATABASE: {symbol}{pair}{timeframe} HAD BEEN UPDATED:')
            return True


# use this to if you wanna dataframe
def dataframe():
    data_frame = pd.read_sql(f'{symbol}{pair}{timeframe}', engine, index_col='time')
    return data_frame


# True range calculator
def tr(data):
    data['previous_close'] = data['close'].shift(1)
    data['high-low'] = abs(data['high'] - data['low'])
    data['high-pc'] = abs(data['high'] - data['previous_close'])
    data['low-pc'] = abs(data['low'] - data['previous_close'])
    t_range = data[['high-low', 'high-pc', 'low-pc']].max(axis=1)
    return t_range


# after true range calculated use this to calculate Average true range
def atr(data):
    data['tr'] = tr(data)
    avg_true_range = data['tr'].rolling(period).mean()
    return avg_true_range


def update_atr(data):
    data_frame = data.tail(period)
    data['tr'] = tr(data_frame)
    avg_true_range = data['tr'].rolling(period).mean()
    return avg_true_range


# Define supertrend for the first time
def supertrends(data):
    atr_multiplier = 3
    hl2 = (data['high'] + data['low']) / 2
    data['atr'] = atr(data)
    upper = hl2 + (atr_multiplier * data['atr'])
    lower = hl2 - (atr_multiplier * data['atr'])
    data['upperband'] = upper
    data['lowerband'] = lower
    data['MA'] = data['close'].rolling(ma).mean()
    data['in_uptrend'] = None
    for current in range(len(data.index)):
        previous = current - 1
        if data['close'][current] > data['upperband'][previous]:
            data['in_uptrend'][current] = True
        elif data['close'][current] < data['lowerband'][previous]:
            data['in_uptrend'][current] = False
        else:
            data['in_uptrend'][current] = data['in_uptrend'][previous]
            if data['in_uptrend'][current] and data['lowerband'][current] < data['lowerband'][previous]:
                data['lowerband'][current] = data['lowerband'][previous]
            if not data['in_uptrend'][current] and data['upperband'][current] > data['upperband'][previous]:
                data['upperband'][current] = data['upperband'][previous]
    return data


# update supertrend use this after you update your database or dataframe
def update_supertrends(data):
    atr_multiplier = 3
    hl2 = (data['high'][-1] + data['low'][-1]) / 2
    data['atr'][-1] = update_atr(data)[-1]
    upper = hl2 + (atr_multiplier * data['atr'][-1])
    lower = hl2 - (atr_multiplier * data['atr'][-1])
    dec = tick_size / 10
    data['upperband'][-1] = binance.helpers.round_step_size(upper, dec)
    data['lowerband'][-1] = binance.helpers.round_step_size(lower, dec)
    data['MA'][-1] = binance.helpers.round_step_size(data['close'].rolling(ma).mean()[-1], dec)
    data['in_uptrend'][-1] = None
    current = -1
    previous = current - 1
    if data['close'][current] > data['upperband'][previous]:
        data['in_uptrend'][current] = True
    elif data['close'][current] < data['lowerband'][previous]:
        data['in_uptrend'][current] = False
    else:
        data['in_uptrend'][current] = data['in_uptrend'][previous]
        if data['in_uptrend'][current] and data['lowerband'][current] < data['lowerband'][previous]:
            data['lowerband'][current] = data['lowerband'][previous]
        if not data['in_uptrend'][current] and data['upperband'][current] > data['upperband'][previous]:
            data['upperband'][current] = data['upperband'][previous]
    return data


# correction check of data
def check_delta_time(data):
    data_frame = data.tail()
    delta_head1 = data_frame.index[0]
    delta_head2 = data_frame.index[1]
    delta_tail1 = data_frame.index[-2]
    delta_tail2 = data_frame.index[-1]

    if delta_head2 - delta_head1 == delta_tail2 - delta_tail1:
        return True
    else:
        return False


had_error = False
condition = True


# RUN IT
def run_bot():
    global had_error, condition
    # update
    while True:
        try:
            if update_database():
                current_data = dataframe()
                show_database(current_data)
                print('DATA CHECK: Passed')
                try:
                    trade(current_data)
                    print('Balance:', exchange.fetch_balance()[pair]['total'],
                          'Free:', exchange.fetch_balance()[pair]['free'])
                except BinanceAPIException as sl_tp_error:
                    if sl_tp_error.code == -2011:
                        print('Unknown order sent')
                        trade(current_data)
                except NetworkError:
                    time.sleep(10)

                condition = True
                print('-' * 69)
                break
            else:
                # create new if unable to update
                try:
                    time.sleep(2)
                    create_database()
                    current_data = dataframe()
                    print('DATA CHECK: Fixed')
                    trade(current_data)
                    print('Balance:', exchange.fetch_balance()[pair]['total'],
                          'Free:', exchange.fetch_balance()[pair]['free'])
                    condition = True
                    print('-' * 69)
                    break
                except Exception as create_data_fail:
                    print('Fail to update database: ', create_data_fail)
                    condition = False
                except NetworkError:
                    time.sleep(10)
        except NetworkError:
            time.sleep(1)
        except ConnectionError:
            time.sleep(1)


def trade(data):
    global order, order_status, tp_order, sl_order, position_amt
    data_f = data
    prices = data_f.close[-1]
    try:
        position_amt = float(client.futures_position_information(symbol=s_symbols)[0]['positionAmt'])
        position_price = float(client.futures_position_information(symbol=s_symbols)[0]['entryPrice'])
    except ConnectionError:
        time.sleep(2)
        position_amt = float(client.futures_position_information(symbol=s_symbols)[0]['positionAmt'])
        position_price = float(client.futures_position_information(symbol=s_symbols)[0]['entryPrice'])

    if position_amt != 0:
        order = client.futures_get_order(symbol=s_symbols, orderId=order['orderId'])

        if sl_order['status'] is None and tp_order['status'] is None:
            print('SL AND TP ARE NONE')
            client.futures_cancel_all_open_orders(symbol=s_symbols)
            time.sleep(1)
            sl_order = create_stop_loss(data_f)

            if data_f.in_uptrend[-1] and data_f.lowerband[-1] < position_price:
                time.sleep(1)
                tp_order = create_take_profit(data_f)

            elif not data_f.in_uptrend[-1] and data_f.upperband[-1] > position_price:
                time.sleep(1)
                tp_order = create_take_profit(data_f)
            else:
                print('TP order: run trend')

        elif tp_order['status'] is None or sl_order['status'] is None:
            print('SL OR TP IS NONE')
            if data_f.in_uptrend[-1]:
                if sl_order['status'] is not None:
                    try:
                        sl_order = client.futures_cancel_order(symbol=s_symbols, orderId=sl_order['orderId'])
                    except BinanceAPIException as cancel_error:
                        if cancel_error.code == -2011:
                            print(cancel_error)
                            client.futures_cancel_all_open_orders(symbol=s_symbols)

                time.sleep(1)
                sl_order = create_stop_loss(data_f)

                if data_f.lowerband[-1] < position_price:
                    try:
                        time.sleep(1)
                        tp_order = create_take_profit(data_f)

                    except TypeError:
                        print('Create TP order failed')
                        tp_order = {'status': None}
                    except BinanceAPIException:
                        print('Create TP order failed')
                        tp_order = {'status': None}
                else:
                    print('TP order: run trend')
                sl_order = client.futures_get_order(symbol=s_symbols, orderId=sl_order['orderId'])

            elif not data_f.in_uptrend[-1]:
                if sl_order['status'] is not None:
                    try:
                        sl_order = client.futures_cancel_order(symbol=s_symbols, orderId=sl_order['orderId'])
                    except BinanceAPIException as cancel_error:
                        if cancel_error.code == -2011:
                            print(cancel_error)
                            client.futures_cancel_all_open_orders(symbol=s_symbols)

                time.sleep(1)
                sl_order = create_stop_loss(data_f)

                if data_f.upperband[-1] > position_price:
                    try:
                        time.sleep(1)
                        tp_order = create_take_profit(data_f)

                    except TypeError:
                        print('Create TP order failed')
                        tp_order = {'status': None}
                    except BinanceAPIException:
                        print('Create TP order failed')
                        tp_order = {'status': None}
                else:
                    print('TP order: run trend')

        elif sl_order['status'] == 'NEW' and tp_order['status'] == 'NEW':
            print('SL AND TP ARE NEW')
            if data_f.in_uptrend[-1]:
                if data_f.lowerband[-1] != data_f.lowerband[-2:]:
                    try:
                        sl_order = client.futures_cancel_order(symbol=s_symbols, orderId=sl_order['orderId'])
                    except BinanceAPIException as cancel_error:
                        if cancel_error.code == -2011:
                            print(cancel_error)
                            client.futures_cancel_all_open_orders(symbol=s_symbols)
                            tp_order = create_take_profit(data_f, tp_order['price'])
                    sl_order = create_stop_loss(data_f)

                # if position_price < data_f.lowerband[-1]:
                #     try:
                #         print('Cancel TP')
                #         tp_order = client.futures_cancel_order(symbol=s_symbols, orderId=tp_order['orderId'])
                #         tp_order = {'status': None}
                #     except BinanceAPIException as cancel_error:
                #         if cancel_error.code == -2011:
                #             client.futures_cancel_all_open_orders(symbol=s_symbols)
                #             sl_order = create_stop_loss(data_f)
                #             print(cancel_error)

            elif not data_f.in_uptrend[-1]:
                if data_f.upperband[-1] != data_f.upperband[-2]:
                    try:
                        sl_order = client.futures_cancel_order(symbol=s_symbols, orderId=sl_order['orderId'])
                    except BinanceAPIException as cancel_error:
                        if cancel_error.code == -2011:
                            print(cancel_error)
                            client.futures_cancel_all_open_orders(symbol=s_symbols)
                    sl_order = create_stop_loss(data_f)

                # if position_price > data_f.upperband[-1]:
                #     try:
                #         print('Cancel TP')
                #         tp_order = client.futures_cancel_order(symbol=s_symbols, orderId=tp_order['orderId'])
                #         tp_order = {'status': None}
                #     except BinanceAPIException as cancel_error:
                #         if cancel_error.code == -2011:
                #             print(cancel_error)

    if data_f.in_uptrend[-1] and not data_f.in_uptrend[-2]:
        state = 'Change to UP TREND'
        print(state)

        # if in position
        if position_amt != 0:
            order = client.futures_get_order(symbol=s_symbols, orderId=order['orderId'])
            sides = order['side']
            # if in short position
            if sides == 'SELL' and position_amt != 0:
                create_close_market()
                client.futures_cancel_all_open_orders(symbol=s_symbols)
                # create open long order
                if exchange.fetch_balance()[pair]['free'] > 1 \
                        and prices > data_f.MA[-1]:
                    order = create_limit(prices, 'buy')
                    sl_order = {'status': None}
                    tp_order = {'status': None}
                    print('Order ID: ', order['orderId'], ' | ',
                          'Price: ', order['price'], ' | ',
                          'STATUS: ', order['status'])
                else:
                    if prices < data_f.MA[-1]:
                        print('Below MA')
                    else:
                        print('Insufficient funds')
            else:
                if tp_order['status'] is not None:
                    try:
                        tp_order = client.futures_cancel_order(symbol=s_symbols, orderId=tp_order['orderId'])
                        tp_order = {'status': None}
                    except BinanceAPIException as cancel_error:
                        if cancel_error.code == -2011:
                            print(cancel_error)
                            client.futures_cancel_all_open_orders(symbol=s_symbols)
                            sl_order = {'status': None}
                            tp_order = {'status': None}
                if sl_order['status'] is not None:
                    try:
                        sl_order = client.futures_cancel_order(symbol=s_symbols, orderId=sl_order['orderId'])
                        sl_order = {'status': None}
                    except BinanceAPIException as cancel_error:
                        if cancel_error.code == -2011:
                            print(cancel_error)
                            client.futures_cancel_all_open_orders(symbol=s_symbols)
                            sl_order = {'status': None}
                            tp_order = {'status': None}
        else:
            client.futures_cancel_all_open_orders(symbol=s_symbols)
            sl_order = {'status': None}
            tp_order = {'status': None}
            order = {'status': None}
            # create open long position
            if exchange.fetch_balance()[pair]['free'] > 1 \
                    and prices > data_f.MA[-1]:
                order = create_limit(prices, 'buy')
                print('Order ID: ', order['orderId'], ' | ',
                      'Price: ', order['price'], ' | ',
                      'STATUS: ', order['status'])

            else:
                if prices < data_f.MA[-1]:
                    print('Below MA')
                else:
                    print('Insufficient funds')

    elif not data_f.in_uptrend[-1] and data_f.in_uptrend[-2]:
        state = 'Change to DOWN TREND'
        print(state)
        if position_amt != 0:
            order = client.futures_get_order(symbol=s_symbols, orderId=order['orderId'])
            sides = order['side']
            if sides == 'BUY':
                create_close_market()
                # open short order
                if exchange.fetch_balance()[pair]['free'] > 1 \
                        and prices < data_f.MA[-1]:
                    order = create_limit(prices, 'sell')
                    sl_order = {'status': None}
                    tp_order = {'status': None}
                    print('Order ID: ', order['orderId'], ' | ',
                          'Price: ', order['price'], ' | ',
                          'STATUS: ', order['status'])
                else:
                    if prices > data_f.MA[-1]:
                        print('Above MA')
                    else:
                        print('Insufficient funds')
            else:
                client.futures_cancel_all_open_orders(symbol=s_symbols)
                sl_order = {'status': None}
                tp_order = {'status': None}
                try:
                    sl_order = create_stop_loss(data_f)
                    tp_order = create_take_profit(data_f)
                    print('Created new SL and TP order')
                except BinanceAPIException:
                    create_close_market()
                    sl_order = {'status': None}
                    tp_order = {'status': None}
        else:
            client.futures_cancel_all_open_orders(symbol=s_symbols)
            sl_order = {'status': None}
            tp_order = {'status': None}
            order = {'status': None}
            # open short order
            if exchange.fetch_balance()[pair]['free'] > 1 \
                    and prices < data_f.MA[-1]:
                client.futures_cancel_all_open_orders(symbol=s_symbols)
                order = create_limit(prices, 'sell')
                sl_order = {'status': None}
                tp_order = {'status': None}
                print('Order ID: ', order['orderId'], ' | ',
                      'Price: ', order['price'], ' | ',
                      'STATUS: ', order['status'])
            else:
                if prices > data_f.MA[-1]:
                    print('Above MA')
                else:
                    print('Insufficient funds')

    else:
        if position_amt != 0:
            print('Order ID: ', order['orderId'], ' | ',
                  'Price: ', order['price'], ' | ',
                  'STATUS: ', order['status'])
            if sl_order['status'] is not None:
                sl_order = client.futures_get_order(symbol=s_symbols, orderId=sl_order['orderId'])
                print('SL order ID: ', sl_order['orderId'], ' | ',
                      'Price: ', sl_order['stopPrice'], ' | ',
                      'STATUS: ', sl_order['status'])
                if tp_order['status'] is not None:
                    tp_order = client.futures_get_order(symbol=s_symbols, orderId=tp_order['orderId'])
                    print('TP order ID: ', tp_order['orderId'], ' | ',
                          'Price: ', tp_order['price'], ' | ',
                          'STATUS: ', tp_order['status'])
            print(f'Unrealized profit({s_symbols}): ',
                  client.futures_position_information(symbol=s_symbols)[0]['unRealizedProfit'], pair)
            print('Total unrealized profit: ',
                  client.futures_account()['totalUnrealizedProfit'], pair)
        else:
            if order['status'] is not None:
                order = client.futures_get_order(symbol=s_symbols, orderId=order['orderId'])
                print('Order ID: ', order['orderId'], ' | ',
                      'Price: ', order['price'], ' | ',
                      'STATUS: ', order['status'])
                if data_f.in_uptrend[-1] and data_f.lowerband[-1] > float(order['price']):
                    order = client.futures_cancel_order(symbol=s_symbols, orderId=order['orderId'])
                    order = {'status': None}
                elif not data_f.in_uptrend[-1] and data_f.upperband[-1] < float(order['price']):
                    order = client.futures_cancel_order(symbol=s_symbols, orderId=order['orderId'])
                    order = {'status': None}
            else:
                print("You're not in position")
                time.sleep(1)


def create_limit(limit_price, side_str):
    global step_size, tick_size, set_amount, min_qty, multiplier
    order_side = ''
    if side_str.upper() == 'BUY':
        order_side = SIDE_BUY
    elif side_str.upper() == 'SELL':
        order_side = SIDE_SELL
    sizing = float(exchange.fetch_balance()[pair]['total']) * set_amount

    od = {'status': None}
    while od['status'] != 'NEW':
        try:
            qty = sizing / limit_price
            if qty < min_qty:
                qty = min_qty
            else:
                qty = binance.helpers.round_step_size(qty, step_size)
            prices = binance.helpers.round_step_size(limit_price, tick_size)
            od = client.futures_create_order(symbol=s_symbols,
                                             type=FUTURE_ORDER_TYPE_LIMIT,
                                             side=order_side,
                                             timeInForce=TIME_IN_FORCE_GTC,
                                             quantity=qty,
                                             price=str(prices))
            print('Limit order had been created')
            try:
                with open(f'order/{symbol}/{symbol}{timeframe}_order.txt', 'w') as order__:
                    order__.write(str(od['orderId']))
            except FileNotFoundError:
                directory = f'order/{symbol}'
                os.mkdir(directory)
                with open(f'order/{symbol}/{symbol}{timeframe}_order.txt', 'w') as order__:
                    order__.write(str(od['orderId']))
            return od
        except BinanceAPIException as order_error:
            print(order_error)
            if order_error.code == -1111 or order_error.code == -1102 or order_error.code == -4164:
                if min_qty % 1 == 0:
                    min_qty += 1
                else:
                    if min_qty >= 0.01:
                        min_qty = min_qty * 10
                    else:
                        min_qty += def_min_qty
                    min_qty = binance.helpers.round_step_size(min_qty, step_size)
                print('Min Qty', min_qty)
            else:
                return {'status': None}
        except ConnectionError:
            time.sleep(10)


def create_stop_loss(data):
    global tick_size

    if position_amt > 0:
        op_side = SIDE_SELL
        sl_price = data.lowerband[-1]
    elif position_amt < 0:
        op_side = SIDE_BUY
        sl_price = data.upperband[-1]
    else:
        op_side = None
        sl_price = None

    sl_qty = abs(position_amt)

    if position_amt != 0:
        try:
            sl_price = binance.helpers.round_step_size(sl_price, tick_size)
            stop_l = client.futures_create_order(symbol=s_symbols,
                                                 side=op_side,
                                                 type=FUTURE_ORDER_TYPE_STOP_MARKET,
                                                 stopLimitTimeInForce=TIME_IN_FORCE_GTC,
                                                 quantity=sl_qty,
                                                 stopPrice=str(sl_price),
                                                 workingType='MARK_PRICE')
            print('SL order had been created')
            try:
                with open(f'order/{symbol}/{symbol}{timeframe}_sl_order.txt', 'w') as sl_file:
                    sl_file.write(str(stop_l['orderId']))
            except FileNotFoundError:
                directory = f'order/{symbol}'
                os.mkdir(directory)
                with open(f'order/{symbol}/{symbol}{timeframe}_sl_order.txt', 'w') as sl_file:
                    sl_file.write(str(stop_l['orderId']))
            return stop_l
        except TimeoutError:
            time.sleep(10)
        except BinanceAPIException as ex_sl:
            print(ex_sl)
            if ex_sl.code == -1111:
                time.sleep(5)
            else:
                stop_l = {'status': None}
                return stop_l


def create_take_profit(data, price=0):
    symbol_info = client.futures_position_information(symbol=s_symbols)[0]
    open_price = float(symbol_info['entryPrice'])
    if position_amt > 0:
        op_side = SIDE_SELL
        band = data.lowerband[-1]
        if price == 0:
            tp_price = abs(((open_price - band) * my_l_target) + open_price)
        else:
            tp_price = price
    elif position_amt < 0:
        op_side = SIDE_BUY
        band = data.upperband[-1]
        if price == 0:
            tp_price = abs(((band - open_price) * my_l_target) - open_price)
        else:
            tp_price = price
    else:
        op_side = None
        tp_price = None

    tp_qty = abs(position_amt)
    tp_price = binance.helpers.round_step_size(tp_price, tick_size)

    try:
        take_p = client.futures_create_order(symbol=s_symbols,
                                             side=op_side,
                                             type=FUTURE_ORDER_TYPE_TAKE_PROFIT,
                                             stopLimitTimeInForce=TIME_IN_FORCE_GTC,
                                             quantity=tp_qty,
                                             stopPrice=str(tp_price),
                                             price=str(tp_price))
        print('TP order had been created')
        try:
            with open(f'order/{symbol}/{symbol}{timeframe}_tp_order.txt', 'w') as tp_file:
                tp_file.write(str(take_p['orderId']))
        except FileNotFoundError:
            directory = f'order/{symbol}'
            os.mkdir(directory)
            with open(f'order/{symbol}/{symbol}{timeframe}_tp_order.txt', 'w') as tp_file:
                tp_file.write(str(take_p['orderId']))
        return take_p
    except BinanceAPIException as tp_error:
        if tp_error.code == -2021:
            print(tp_error)
            create_close_market()
        else:
            print(tp_error)


def create_close_market():
    global order
    market_qty = abs(position_amt)
    try:
        if position_amt != 0:
            if position_amt < 0:
                order_side = SIDE_BUY
            else:
                order_side = SIDE_SELL

            client.futures_create_order(symbol=s_symbols,
                                        type=FUTURE_ORDER_TYPE_MARKET,
                                        side=order_side,
                                        quantity=market_qty)
            print('Order Closed')
            order = {'status': None}
    except BinanceAPIException as ex_market:
        print('CLOSE POSITION ERROR: ', ex_market)
        time.sleep(1)
        order = {'status': None}


def check_real_time():
    global order, sl_order, tp_order, position_amt
    try:
        position_amt = float(client.futures_position_information(symbol=s_symbols)[0]['positionAmt'])
    except NetworkError or ConnectionError or ChunkedEncodingError:
        time.sleep(2)
        position_amt = float(client.futures_position_information(symbol=s_symbols)[0]['positionAmt'])
    if order['status'] is not None:
        try:
            if position_amt == 0 and order['status'] != 'NEW':
                client.futures_cancel_all_open_orders(symbol=s_symbols)
                sl_order = {'status': None}
                tp_order = {'status': None}
                order = {'status': None}
        except NetworkError:
            pass
        except ConnectionError:
            pass
    elif order['status'] is None:
        try:
            if position_amt == 0:
                open_order_num = client.futures_get_open_orders(symbol=s_symbols)
                if len(open_order_num) > 0:
                    client.futures_cancel_all_open_orders(symbol=s_symbols)
                    sl_order = {'status': None}
                    tp_order = {'status': None}
                    order = {'status': None}
            else:
                pass
        except NetworkError:
            pass
        except ConnectionError:
            pass


# import asyncio
warnings.filterwarnings('ignore')
# pd.set_option('display.max_columns', None)

# line_token = 'khje0YWxrkoWBKzCYBpYPn7FceIyVSYbv0PcIeJWef0'
# line = songline.Sendline(line_token)

API = 'NNyRjpCE7ZpQDtVHPYqbBz9JUp1wxcDcwV0f3YyXzxJTIWZfbj6EnKXEyITRfTHB'
SECRET = '9HHMYPzOsPNntMUviaXbVOr4GUJOtRyybcIyeqMJmbz3N9R5ph8C3tbGs4sTuRPt'
exchange = ccxt.binanceusdm({'apiKey': API, 'secret': SECRET})
client = Client(API, SECRET)
status = exchange.fetch_status()
print('CONNECTION: ', status['status'])

while True:
    try:
        exchange.fetch_balance()
        break
    except Exception as ex:
        print(f'ERROR @ Connection phase: {ex}')
        time.sleep(30)

# define symbols
symbols = None
symbol = None
s_symbols = None
while symbols not in exchange.symbols:
    # sym = ['ADA', 'BNB', 'ETH']
    symbol = input('Symbol: ').upper()
    # symbol = 'ADA'

    pair = 'USDT'
    symbols = f'{symbol}/{pair}'
    s_symbols = f'{symbol}{pair}'
    time.sleep(0.5)
    if symbol == 'DIR':
        pprint(exchange.symbols)

# define timeframe
timeframe_set = ['1m', '5m', '15m', '1h', '4h', '1d']
timeframe = None
while timeframe not in timeframe_set:
    timeframe = input('Timeframe(1m, 15m, 1h, 4h, 1d): ')

my_l_target = 0
while my_l_target <= 0:
    try:
        my_l_target = float(input('Target(ratio) : '))

    except ValueError:
        print('Value error')

# ATR period **
my_period = str(input('(ATR)Period: '))
if int(my_period) in range(1, 21):
    period = int(my_period)
elif my_period == '':
    period = 10

ma = int(input('Moving average: '))

# SQL engine
engine = sqlalchemy.create_engine('sqlite:///supertrend.db')
bands = ''

leverage = ''
while leverage == '':
    # set leverage
    set_leverage = int(input('Leverage: '))
    try:
        leverage = client.futures_change_leverage(symbol=s_symbols, leverage=set_leverage)
    except BinanceAPIException as leverage_error:
        if leverage_error.code == -4028:
            print('Leverage is not valid')

# change margin type to isolate
position_amt = float(client.futures_position_information(symbol=s_symbols)[0]['positionAmt'])
margin_type = ''
while margin_type != 'isolated':
    margin_type = client.futures_position_information(symbol=s_symbols)[0]['marginType']
    if margin_type != 'isolated':
        try:
            client.futures_change_margin_type(symbol=s_symbols, marginType='ISOLATED')
        except Exception as e:
            print(e)
            time.sleep(60)
    print('Margin type: ', margin_type.upper())

answers = None
while answers is None:
    answers = str(input("If you're in open position do you want to used your previous order?(Y/N or 0)\n: "))

    if answers.upper() == 'Y':
        try:
            with open(f'order/{symbol}/{symbol}{timeframe}_order.txt', 'r') as file:
                order = client.futures_get_order(symbol=s_symbols, orderId=int(file.read()))

            if order['status'] == 'FILLED' and position_amt != 0:
                with open(f'oder/{symbol}/{symbol}{timeframe}_order.txt', 'w') as order_file:
                    order_file.write(str(order['orderId']))
                print('Order ID: ', order['orderId'], ' | ',
                      'Price: ', order['price'], ' | ',
                      'STATUS: ', order['status'])
            elif order['status'] == 'NEW' and position_amt == 0:
                with open(f'order/{symbol}/{symbol}{timeframe}_order.txt', 'w') as order_file:
                    order_file.write(str(order['orderId']))
                print('Order ID: ', order['orderId'], ' | ',
                      'Price: ', order['price'], ' | ',
                      'STATUS: ', order['status'])
            else:
                order = {'status': None}
        except FileNotFoundError:
            order = {'status': None}
        except ValueError:
            print(ValueError)
        try:
            with open(f'order/{symbol}/{symbol}{timeframe}_sl_order.txt', 'r') as file:
                sl_order = client.futures_get_order(symbol=s_symbols, orderId=int(file.read()))

            if sl_order['status'] == 'FILLED':
                print('SL order ID: ', sl_order['orderId'], ' | ',
                      'Price: ', sl_order['stopPrice'], ' | ',
                      'STATUS: ', sl_order['status'])
                sl_order = {'status': None}
                order = {'status': None}
            elif sl_order['status'] == 'NEW':
                print('SL order ID: ', sl_order['orderId'], ' | ',
                      'Price: ', sl_order['stopPrice'], ' | ',
                      'STATUS: ', sl_order['status'])
            else:
                sl_order = {'status': None}
        except FileNotFoundError:
            sl_order = {'status': None}
        except ValueError:
            print(ValueError)
        try:
            with open(f'order/{symbol}/{symbol}{timeframe}_tp_order.txt', 'r') as file:
                tp_order = client.futures_get_order(symbol=s_symbols, orderId=int(file.read()))

            if tp_order['status'] == 'FILLED':
                print('TP order ID: ', tp_order['orderId'], ' | ',
                      'Price: ', tp_order['price'], ' | ',
                      'STATUS: ', tp_order['status'])
                tp_order = {'status': None}
                sl_order = {'status': None}
                order = {'status': None}
            elif tp_order['status'] == 'NEW':
                print('TP order ID: ', tp_order['orderId'], ' | ',
                      'Price: ', tp_order['price'], ' | ',
                      'STATUS: ', tp_order['status'])
            else:
                tp_order = {'status': None}
        except FileNotFoundError:
            tp_order = {'status': None}
        except ValueError:
            print(ValueError)

    elif answers == '0':
        order = client.futures_get_order(symbol=s_symbols, orderId=int(input('Enter your order ID\n: ')))
        tp_order = {'status': None}
        sl_order = {'status': None}
    elif answers.upper() == 'N':
        client.futures_cancel_all_open_orders(symbol=s_symbols)
        order = {'status': None}
        order_status = None
        tp_order = {'status': None}
        sl_order = {'status': None}
    else:
        print('Please enter your answer')
        answers = None

info = client.get_symbol_info(s_symbols)
step_size = float(info['filters'][2]['stepSize'])
tick_size = float(info['filters'][0]['tickSize'])
min_qty = float(info['filters'][2]['minQty'])
def_min = min_qty

def_min_qty = float(info['filters'][2]['minQty'])
set_amount = 0.1
multiplier = 1

triggered = False
create_data = create_database()
df = dataframe().tail(100)
if create_data:
    show_database(df)
    trade(df)
while not triggered:
    current_time = datetime.today()
    current_minutes = current_time.strftime('%M')
    current_hours = current_time.strftime('%H')
    current_days = current_time.strftime('%d')
    if timeframe[-1] in 'm':
        if current_minutes in ['00', '15', '30', '45'] and timeframe == '15m':
            # try:
            if create_data:
                update_database()
                trade(dataframe().tail(100))
                schedule.every(15).minutes.at(':00').do(run_bot)
                triggered = True
                print('15m Triggered')
            else:
                time.sleep(10)
                create_data = create_database()

            # except Exception as e:
            #     print(f'ERROR @ Trigger phase: {e}')
            #     time.sleep(60)

        elif timeframe == '1m':
            if create_data:
                schedule.every().minute.at(':00').do(run_bot)
                triggered = True
                print('1m Triggered')
            else:
                time.sleep(10)
                create_data = create_database()

        if current_minutes[-1] in ['0', '5'] and timeframe == '5m':
            # try:
            if create_data:
                update_database()
                trade(dataframe().tail(100))
                schedule.every(5).minutes.at(':00').do(run_bot)
                triggered = True
                print('5m Triggered')
            else:
                time.sleep(10)
                create_data = create_database()

            # except Exception as e:
            #     print(f'ERROR @ Trigger phase: {e}')
            #     time.sleep(60)

    elif timeframe[-1] in 'h':

        if timeframe == '1h':
            # try:
            if create_data:
                schedule.every().hour.at(':00').do(run_bot)
                triggered = True
                print('1h Triggered')
                # print(scalping_position)
                break
            else:
                time.sleep(10)
                create_data = create_database()

            # except Exception as e:
            #     print(f'ERROR: {e}')
            #     time.sleep(60)

        elif timeframe == '4h' and current_hours in ['00', '04', '08', '12', '16', '20']:
            if create_data:
                update_database()
                trade(dataframe().tail(100))
                schedule.every(4).hours.at(':00').do(run_bot)
                triggered = True
                print('4h Triggered')
            else:
                time.sleep(10)
                create_data = create_database()

    elif timeframe[-1] == 'd':
        if create_database():
            schedule.every().day.at('00:00').do(run_bot)
            triggered = True
            print('1d Triggered')
        else:
            time.sleep(10)
            create_data = create_database()

print('Balance:', exchange.fetch_balance()['USDT']['total'],
      'Free:', exchange.fetch_balance()['USDT']['free'])
print('-' * 63)

while condition:

    schedule.run_pending()

    current_minutes = datetime.today().strftime('%M')
    current_sec = datetime.today().strftime('%S')
    if timeframe != '1m':
        if current_minutes[-1] in ['9', '0', '4', '5']:
            if current_sec == '59' or current_sec == '00':
                time.sleep(0.1)
            else:
                check_real_time()
                time.sleep(5)
        else:
            check_real_time()
            time.sleep(10)
    else:
        if int(current_sec) < 54:
            check_real_time()
            time.sleep(5)
        else:
            time.sleep(0.1)
