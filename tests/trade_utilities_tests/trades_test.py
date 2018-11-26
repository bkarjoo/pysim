from trade_utilities.trades import Trades
from trade_utilities.trade import Trade
from trade_utilities.execution import Execution
import datetime


ts = Trades()


e1 = Execution(100, 'IBM', 55.4, datetime.datetime(2011,11,11,9,30))
ts.add_execution(e1)
print str(ts.get_open_trade('IBM')[0])

e2 = Execution(100, 'IBM', 55.5, datetime.datetime(2011,11,11,9,30))
ts.add_execution(e2)
print str(ts.get_open_trade('IBM')[0])

e3 = Execution(-100, 'IBM', 55.5, datetime.datetime(2011,11,11,9,30))
ts.add_execution(e3)
print str(ts.get_open_trade('IBM')[0])

e4 = Execution(-100, 'IBM', 55.5, datetime.datetime(2011,11,11,9,30))
ts.add_execution(e4)
print str(ts.get_closed_trade('IBM')[0])

e1 = Execution(100, 'IBM', 55.5, datetime.datetime(2011,11,11,9,30))
ts.add_execution(e1)
print str(ts.get_open_trade('IBM')[0])

e2 = Execution(100, 'IBM', 55.5, datetime.datetime(2011,11,11,9,30))
ts.add_execution(e2)
print str(ts.get_open_trade('IBM')[0])

e3 = Execution(-100, 'IBM', 55.5, datetime.datetime(2011,11,11,9,30))
ts.add_execution(e3)
print str(ts.get_open_trade('IBM')[0])

e4 = Execution(-100, 'IBM', 55.5, datetime.datetime(2011,11,11,9,30))
ts.add_execution(e4)
print str(ts.get_closed_trade('IBM')[1])

e1 = Execution(100, 'AAPL', 55.5, datetime.datetime(2011,11,11,9,30))
ts.add_execution(e1)
print str(ts.get_open_trade('AAPL')[0])

e2 = Execution(100, 'AAPL', 55.5, datetime.datetime(2011,11,11,9,30))
ts.add_execution(e2)
print str(ts.get_open_trade('AAPL')[0])

e3 = Execution(-100, 'IBM', 55.5, datetime.datetime(2011,11,11,9,30))
ts.add_execution(e3)
print str(ts.get_open_trade('IBM')[0])

e4 = Execution(-100, 'IBM', 55.5, datetime.datetime(2011,11,11,9,30))
ts.add_execution(e4)
print str(ts.get_open_trade('IBM')[0])

print 'number of trades', ts.number_of_trades()
print 'number of winners', ts.number_of_winners()
print 'number of losers', ts.number_of_losers()
print 'win rate', ts.win_rate()

# if t.is_closed:
#     t2 = t
#     t = Trade()
# if not t.is_closed:
#     e5 = Execution(-100, 'IBM', 55.5, datetime.datetime(2011,11,11,9,30))
#     t.add_execution(e5)
#
#
# print t.position()
# print t2.position()
