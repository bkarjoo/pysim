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
    else:
        raise ValueError("Order cannot have 0 as its quantity.")


def check_fills(row):

    while done_away_orders.has_order():
        o = done_away_orders.pop_order()
        day_return.add_transaction(o.qty, o.price)
        e = Execution(o.qty, o.symbol, o.price, row.name)
        executions.append(e)
        trades.add_execution(e)



    orders = open_orders.get_open_orders(row['symbol'])
    if orders is not None:
        for o in orders[:]:
            if is_fill(o, row):
                record_fill(o, row)
                orders.remove(o)


def check_donaways_eod(date_i):
    while done_away_orders.has_order():
        o = done_away_orders.pop_order()
        day_return.add_transaction(o.qty, o.price)
        e = Execution(o.qty, o.symbol, o.price, date_i)
        executions.append(e)
        trades.add_execution(e)
