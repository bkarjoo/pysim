from trade_utilities.daily_return import DailyReturn
import datetime
from trade_utilities.position import Position

prev = datetime.datetime(2018, 10, 29)
today = datetime.datetime(2018, 10, 30)

position_list = {'AAPL': Position('AAPL', 100), 'GE': Position('GE', -1000)}

dr = DailyReturn()
dr.reset(prev, today, position_list)

print dr.transactions
dr.close_positions(position_list)
print dr.transactions

print dr.get_transactions_sum()
