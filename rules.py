from trade_utilities.functions import *
import datetime
import config
import sqlite3
import math
import pickle
from trade_utilities.bar_dictionary import BarDictionary
import data_utilities.eod as eod

strategy_name = 'twitter sentiment'
commission_per_share = 0.007
start_date = '2017-07-13'
end_date = '2018-06-13'
previous_date = datetime.datetime(2017, 7, 12)
todays_date = datetime.datetime(2017, 7, 13)
break_out_of_loop = False # set to true if no longer positions are possible
reset_after_opening = False # get a reduced basket for just the filled positions at 9:30
use_intraday = False

today_basket = []

# Custom Data
earnings_db_path = '{}/custom/earning.db'.format(config.data_root)
sentiment_db_path = '{}/custom/sentiment.db'.format(config.data_root)
earnings_db_connection = None
sentiment_db_connection = None

potential_longs = None
potential_shorts = None

bars = BarDictionary()
bloomberg_eod = None


def reset_reset_after_opening():
    global reset_after_opening
    reset_after_opening = False

# checks if the stock has earnings,
# can take a string date or a datetime date
def has_earnings(symbol, date):
    if type(date) is datetime.datetime:
        date = str(date.date())
    cursor = earnings_db_connection.cursor()
    cursor.execute("SELECT * FROM earnings WHERE symbol=? and date=?", (symbol, date))
    rows = cursor.fetchall()
    if len(rows) > 1:
        raise ValueError('Database seems to be corrupt, a symbol can not have 2 earnings on one day')
    return len(rows) == 1


def earnings_basket(date):
    if type(date) is datetime.datetime:
        date = str(date.date())
    cursor = earnings_db_connection.cursor()
    cursor.execute("SELECT symbol FROM earnings WHERE date=?", (date,))
    rows = cursor.fetchall()
    return [item[0] for item in rows]


# finds all the stocks that are due for a buy or sell
def twitter_sentiment_zscore(date, z_score_value, greater_than = True):
    if type(date) is datetime.datetime:
        date = str(date.date())
    cursor = sentiment_db_connection.cursor()
    if greater_than:
        cursor.execute("SELECT symbol FROM twitter WHERE date=? and z_score>?", (date, z_score_value))
    else:
        cursor.execute("SELECT symbol FROM twitter WHERE date=? and z_score<?", (date, z_score_value))
    rows = cursor.fetchall()
    return [item[0] for item in rows]


def clean_list(symbol_list):
    return [symbol.replace('/', '.') for symbol in symbol_list]


# the symbols to trade on any given date
def basket(date_time):
    # to determine if there's a trade the zscore of the symbol on that date must be > 1
    z_score_greater_than_1 = clean_list(twitter_sentiment_zscore(date_time, 1, True))
    z_score_less_than_neg1 = clean_list(twitter_sentiment_zscore(date_time, -1, False))
    earnings_symbols = clean_list(earnings_basket(date_time))
    # remove earnings from baskets
    global potential_longs
    global potential_shorts
    potential_longs = list(set(z_score_greater_than_1) - set(earnings_symbols))
    filtered = []
    for symbol in potential_longs:
        stats = eod.get_stats(symbol, date_time, 20)
        if stats.adr > 1: filtered.append(symbol)
    potential_longs = filtered[:]
    potential_shorts = list(set(z_score_less_than_neg1) - set(earnings_symbols))
    filtered = []
    for symbol in potential_shorts:
        stats = eod.get_stats(symbol, date_time, 20)
        if stats.adr > 1: filtered.append(symbol)
        potential_shorts = filtered[:]
    return potential_longs + potential_shorts


def on_run_start():
    global earnings_db_connection
    global sentiment_db_connection
    try:
        earnings_db_connection = sqlite3.connect(earnings_db_path)
        sentiment_db_connection = sqlite3.connect(sentiment_db_path)
    except sqlite3.Error as e:
        print e
    global bloomberg_eod
    f = open('{}/custom/bb_close.pkl'.format(config.data_root))
    bloomberg_eod = pickle.load(f)
    f.close()
    global reset_after_opening
    reset_after_opening = False


def on_new_day(date):
    print __name__, 'on new day', date
    global todays_date
    todays_date = date
    global break_out_of_loop
    break_out_of_loop = False
    global reset_after_opening
    reset_after_opening = False
    bars.reset()



# called every minute before new data arrives and even if no new data arrives for that minute
def on_new_minute(date_time):
    global break_out_of_loop
    # print date_time.time() == datetime.time(9, 28)
    if date_time.time() == datetime.time(9, 28):
        for symbol in potential_longs:
            if bars.has_data_today(symbol, date_time):
                try:
                    # eod = bloomberg_eod[symbol]
                    eod_bar = eod.get_eod_bars(symbol, 10, date_time)

                    close_price = eod_bar[previous_date.date():previous_date.date()]['close'][0]
                    open_price = eod_bar[todays_date.date():todays_date.date()]['open'][0]
                    if math.isnan(open_price): continue
                    pre_market_last = bars.get_close_price(symbol)

                    # if pre_market_last > close_price:
                    #     if pre_market_last > close_price * 1.01:
                    #         pass
                    #     else:
                    #         #print round(10000/pre_market_last, 0), symbol, open_price
                    #         done_away(round(10000/pre_market_last, 0), symbol, open_price)

                    done_away(round(10000 / pre_market_last, 0), symbol, open_price)
                except Exception as e:
                    print 'ERROR', e.message
                    pass
            else:
                try:
                    # eod = bloomberg_eod[symbol]
                    eod_bar = eod.get_eod_bars(symbol, 10, date_time)

                    open_price = eod_bar[todays_date.date():todays_date.date()]['open'][0]
                    if math.isnan(open_price): continue
                    done_away(round(10000 / open_price, 0), symbol, open_price)
                except Exception as e:
                    print 'ERROR', e.message
                    pass

        for symbol in potential_shorts:
            if bars.has_data_today(symbol, date_time):
                try:
                    # eod = bloomberg_eod[symbol]
                    eod_bar = eod.get_eod_bars(symbol, 10, date_time)

                    close_price = eod_bar[previous_date.date():previous_date.date()]['close'][0]
                    open_price = eod_bar[todays_date.date():todays_date.date()]['open'][0]
                    if math.isnan(open_price): continue
                    pre_market_last = bars.get_close_price(symbol)

                    # if pre_market_last < close_price:
                    #     if pre_market_last < close_price * .99:
                    #         pass
                    #     else:
                    #         done_away(round(-10000/pre_market_last, 0), symbol, open_price)
                    #         #print round(-10000/pre_market_last, 0), symbol, open_price

                    done_away(round(-10000 / pre_market_last, 0), symbol, open_price)
                except Exception as e:
                    print 'ERROR', e.message
                    pass
            else:
                try:
                    # eod = bloomberg_eod[symbol]
                    eod_bar = eod.get_eod_bars(symbol, 10, date_time)

                    open_price = eod_bar[todays_date.date():todays_date.date()]['open'][0]
                    if math.isnan(open_price): continue
                    done_away(round(-10000 / open_price, 0), symbol, open_price)
                except Exception as e:
                    print 'ERROR', e.message
                    pass
    elif date_time.time() == datetime.time(9, 30):
        break_out_of_loop = True
    # elif date_time.time() == datetime.time(9, 30):
    #     global break_out_of_loop
    #     break_out_of_loop = True


def on_new_bar(date_time, symbol, open_price, high_price, low_price, close_price, volume):
    global reset_after_opening
    if date_time.time() <= datetime.time(9, 28):
        bars.add_row(symbol, date_time, open_price, high_price, low_price, close_price, volume)


    # if date_time.time() == datetime.time(9,35):
    #     if trades.has_position(symbol):
    #
    #         trade = trades.get_open_trade(symbol)
    #         ep = trade.average_entry_price()
    #         if trade.side == 'Long':
    #
    #             sell(abs(trade.position()), symbol, ep * 1.01)
    #         elif trade.side == 'Short':
    #
    #             buy(abs(trade.position()), symbol, ep * .99)


def on_day_end():

    for symbol, pos in trades.get_open_position_list():
        # eod = bloomberg_eod[symbol]
        eod_bar = eod.get_eod_bars(symbol, 10, todays_date)

        close_price = eod_bar[todays_date.date():todays_date.date()]['close'][0]
        done_away(pos * -1, symbol, close_price)

    global previous_date
    previous_date = todays_date


def on_run_end():
    earnings_db_connection.close()
    sentiment_db_connection.close()

    pass
