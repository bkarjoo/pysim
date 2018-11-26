from kibot.kibot_downloader import *
import rules
from trade_utilities.simulator import *
from trade_utilities.dates import *
import copy
import numpy as np
import pickle
import time


# TODO move this to time utilities
def add_minute(tm):
    fulldate = datetime.datetime(100,1,1,tm.hour, tm.minute, tm.second)
    fulldate = fulldate + datetime.timedelta(minutes=1)
    return fulldate.time()


# TODO move this to time utilities
def time_difference(t1, t2):
    fulldate1 = datetime.datetime(100,1,1,t1.hour, t1.minute, t1.second)
    fulldate2 = datetime.datetime(100, 1, 1, t2.hour, t2.minute, t2.second)
    fulldate3 = fulldate1 - fulldate2
    return fulldate3


def bars_loop(time_tracker, dfc):
    time_recorded = False
    for index, row in dfc.iterrows():

        check_fills(row)

        bar_datetime = index.to_pydatetime()
        while time_difference(bar_datetime, time_tracker) != datetime.timedelta(0):
            # time has advanced
            time_tracker = add_minute(time_tracker)

            # -------------------------------------------------------------------
            rules.on_new_minute(datetime.datetime(bar_datetime.year, bar_datetime.month, bar_datetime.day,
                                                  time_tracker.hour, time_tracker.minute))
            # -------------------------------------------------------------------

        rules.on_new_bar(bar_datetime, row['symbol'], row['open'],
                         row['high'], row['low'], row['close'], row['volume'])

        if rules.break_out_of_loop:
            break
        if rules.reset_after_opening:
            if time_tracker == datetime.time(9,30):
                break


start_time = time.time()
start_dt = datetime.datetime.strptime(rules.start_date, '%Y-%m-%d')
end_dt = datetime.datetime.strptime(rules.end_date, '%Y-%m-%d')

# -------------------------------------------------------------------
rules.on_run_start()
# -------------------------------------------------------------------
previous_date = None
date_i = datetime.datetime.strptime(rules.start_date, '%Y-%m-%d')
while is_weekend(date_i) or date_i in us_holidays:
    date_i += datetime.timedelta(days=1)

while date_i <= end_dt:
    day_return.reset(previous_date, date_i, trades.get_open_position_list())
    symbols = rules.basket(date_i)
    position_list = trades.get_open_position_symbol_list()
    data_req_list = symbols + list(set(position_list) - set(symbols))

    # -------------------------------------------------------------------
    rules.on_new_day(date_i)
    # -------------------------------------------------------------------

    if len(data_req_list) > 0:
        dfc = request_minute_datas_for_one_day(data_req_list, date_i)
    else:
        dfc = pd.DataFrame()

    time_tracker = datetime.time(4)

    # print("pre_loop_time {}".format(time.time() - pre_loop_time_start))
    print 'Length of DFC = {}'.format(len(dfc))
    loop_start_time = time.time()
    bars_loop(time_tracker, dfc)
    if rules.reset_after_opening:
        rules.reset_reset_after_opening()
        dfc = request_minute_datas_for_one_day(trades.get_open_position_symbol_list(), date_i);
        bars_loop(time_tracker, dfc);

    del dfc
    print("loop_time {}".format(time.time() - loop_start_time))

    # -------------------------------------------------------------------
    rules.on_day_end()
    # -------------------------------------------------------------------

    open_orders.reset()
    check_donaways_eod(date_i)

    day_return.close_positions(trades.get_open_position_list())
    daily_returns.append(copy.deepcopy(day_return))

    previous_date = date_i
    date_i += datetime.timedelta(days=1)
    while is_weekend(date_i) or date_i in us_holidays:
        date_i += datetime.timedelta(days=1)

# -------------------------------------------------------------------
rules.on_run_end()
# -------------------------------------------------------------------


with open('execution.csv', 'w') as f:
    for item in executions.executions:
        f.write(str(item) + '\n')
filehandler = open("executions.pkl", "wb")
pickle.dump(executions.executions, filehandler)
filehandler.close()


with open('daily_returns.csv', 'w') as f:
    for item in daily_returns.returns:
        f.write('{},{},\n'.format((item[0].date()), str(round(item[1],2))))


# TODO Save stats in sqlite database
print 'gross = ', daily_returns.gross()
print 'net = ', daily_returns.net(executions.shares_traded(), rules.commission_per_share)
print 'shares = ', trades.total_shares()
print 'shares traded long = ', trades.closed_shares_long()
print 'shares traded short = ', trades.closed_shares_short()
print 'pnl on long trades = ', trades.pnl_long_trades()
print 'pnl on short trades = ', trades.pnl_short_trades()
print 'max exposure = ', trades.max_exposure
print 'max exposure long = ', trades.max_exposure_long
print 'max exposure short = ', trades.max_exposure_short
print 'number of trades = ', trades.number_of_trades()
print 'number of winners = ', trades.number_of_winners()
print 'number of losers = ', trades.number_of_losers()
print 'win/loss ratio = ', trades.win_rate()
print 'biggest winner = ', trades.biggest_winner()
print 'biggest loser = ', trades.biggest_loser()
print 'biggest winner/biggest loser ratio', 0 if trades.biggest_loser() == 0 \
    else round(trades.biggest_winner()/float(trades.biggest_loser()),2)
print 'average cents per share', trades.cps()
print 'average winner cps', trades.long_cps()
print 'average loser cps', trades.short_cps()


# TODO show the equity curve with matplotlib
x = np.array([item[0].date() for item in daily_returns.returns])
y = np.array([item[1] for item in daily_returns.returns])
#plt.plot(x, y)
#pickle the plot for jupyter user
filehandler = open("equity_curve.pkl", "wb")
pickle.dump((x, y), filehandler)
filehandler.close()

print("--- %s seconds --- {}".format(time.time() - start_time))