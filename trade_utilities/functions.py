from order import Order
from daily_returns import DailyReturns
from executions import Executions
from trades import Trades
from orders import Orders
from daily_return import DailyReturn
from data_utilities import eod
import datetime


open_orders = Orders()
done_away_orders = Orders()
executions = Executions()
daily_returns = DailyReturns()
trades = Trades()
day_return = DailyReturn()


# create an order object and add it to open orders list
def buy(qty, symbol, price):
    o = Order(qty, symbol, price)
    open_orders.append(o)


def sell(qty, symbol, price):
    o = Order(qty * -1, symbol, price)
    open_orders.append(o)


#using this to force opening trade fills on bloomberg
def done_away(qty, symbol, price):
    o = Order(qty, symbol, price)
    done_away_orders.append(o)



def get_position(symbol):
    t = trades.get_open_trade(symbol)
    return 0 if t is None else t.position()


def has_position(symbol):
    return get_position(symbol) != 0


def close_position(symbol):
    # TODO implement
    pass

# will average the volume of the number of days ending on the trading day before request date
# def adv(symbol, number_of_days, request_date):
#     return eod.average_daily_range(symbol, number_of_days, request_date)
#
#
# def adr(symbol, number_of_days, request_date):
#     return eod.average_daily_range(symbol, number_of_days, request_date)
#
#
# print adr('AAPL', 90, datetime.datetime(2018, 1, 2))