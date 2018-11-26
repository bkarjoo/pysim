# checks for fills
from functions import *
from execution import Execution

import datetime





def record_fill(o, row):
    fill_price = 0.0
    if o.price == 0:
        fill_price = float(row['open'])
    else:
        if o.qty > 0:
            fill_price = min(o.price, row['open'])
        elif o.qty < 0:
            fill_price = max(o.price, row['open'])

    day_return.add_transaction(o.qty, fill_price)
    e = Execution(o.qty, o.symbol, fill_price, row.name)
    executions.append(e)
    trades.add_execution(e)


def is_fill(o, row):

    if o.qty == 0:
        raise ValueError("Order cannot have 0 as its quantity.")
    if o.qty > 0:
        if o.price == 0:
            return True
        elif row['low'] < o.price:
            return True
        else:
            return False
    elif o.qty < 0:
        if o.price == 0:
            return True
        elif row['high'] > o.price:
            return True
        else:
            return False


def check_fills(row):
    orders = done_away_orders.get_open_orders(row['symbol'])
    if orders is not None:
        for o in orders[:]:
            fill_price = o.price
            day_return.add_transaction(o.qty, fill_price)
            e = Execution(o.qty, o.symbol, fill_price, row.name)
            executions.append(e)
            trades.add_execution(e)
            orders.remove(o)

    orders = open_orders.get_open_orders(row['symbol'])
    if orders is not None:
        for o in orders[:]:
            if is_fill(o, row):
                record_fill(o, row)
                orders.remove(o)


def check_donaways_eod(date_i):
    for symbol in done_away_orders.keys():
        orders = done_away_orders.get_open_orders(symbol)
        for o in orders[:]:
            fill_price = o.price
            day_return.add_transaction(o.qty, fill_price)
            e = Execution(o.qty, o.symbol, fill_price, date_i + datetime.timedelta(hours=16))
            executions.append(e)
            trades.add_execution(e)
            orders.remove(o)


