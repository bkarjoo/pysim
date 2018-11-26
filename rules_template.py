from trade_utilities.functions import *
import datetime

strategy_name = 'template'
commission_per_share = 0.007
start_date = '2018-09-29'
end_date = '2018-10-05'
break_out_of_loop = False # set to true if no longer positions are possible
reset_after_opening = False # get a reduced basket for just the filled positions at 9:30


# the symbols to trade on any given date
def basket(dt = None):
    return ['AAPL', 'IBM']


def on_run_start():
    pass


def on_new_day():
    pass


# called every minute even if there's no data on that minute
def on_new_minute(date_time):
    pass


def on_new_bar(date_time, symbol, open_price, high_price, low_price, close_price, volume):

    if get_position(symbol) > 0:
        if date_time.time() > datetime.time(15, 0):
            print 'selling', 100, symbol, close_price*.95, date_time
            sell(100, symbol, close_price * .95)
    else:
        if date_time.time() < datetime.time(11, 0) \
                and get_position(symbol) == 0:
            print 'buying', 100, symbol, close_price*1.01, date_time
            buy(100, symbol, close_price * 1.01)


def on_day_end():
    pass


def on_run_end():
    pass
