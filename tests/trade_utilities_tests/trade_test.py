from trade_utilities.trade import Trade
from trade_utilities.execution import Execution
import datetime


t = Trade()

e1 = Execution(-100, 'IBM', 55, datetime.datetime(2011,11,11,9,30))
t.add_execution(e1)
print t.position()

e2 = Execution(-100, 'IBM', 57, datetime.datetime(2011,11,11,9,30))
t.add_execution(e2)
print t.position()

e3 = Execution(-100, 'IBM', 54, datetime.datetime(2011,11,11,9,30))
t.add_execution(e3)
print t.position()

e4 = Execution(-100, 'IBM', 58, datetime.datetime(2011,11,11,9,30))
t.add_execution(e4)
print t.position()

# if t.is_closed:
#     t2 = t
#     t = Trade()
# if not t.is_closed:
#     e5 = Execution(-100, 'IBM', 55.5, datetime.datetime(2011,11,11,9,30))
#     t.add_execution(e5)


print t.position()
#print t2.position()
print t.average_entry_price()