from trade_utilities.simulator import *
from kibot.kibot_downloader import *
from trade_utilities.order import Order

log_on()
df = request_history_as_data_frame('AAPL', 1, None, '2018-10-30', '2018-10-30')
# 2018-10-30 09:30:00  211.15  211.2  209.27  209.6824  771859   AAPL
df = df['2018-10-30 9:30:00':'2018-10-30 9:30:00']
row = df.iloc[0]

o1 = Order(100, 'AAPL', 0)
o2 = Order(-100, 'AAPL', 211.16)
o3 = Order(100, 'AAPL', 50)
o4 = Order(-100, 'AAPL', 500)
o5 = Order(100, 'AAPL', 500)
o6 = Order(-100, 'AAPL', 50)

open_orders.append(o1)
open_orders.append(o2)
open_orders.append(o3)
open_orders.append(o4)


def record_fill_test():
    record_fill(o1, row)
    record_fill(o2, row)
    record_fill(o5, row)
    record_fill(o6, row)
    t1 = len(executions) == 4
    t2 = executions[0].price == row['open']
    t3 = executions[1].price == o2.price
    t4 = executions[2].price == row['open']
    t5 = executions[3].price == row['open']
    return t1 and t2 and t3 and t4 and t5


def is_fill_test():
    f1 = is_fill(o1, row)
    f2 = is_fill(o2, row)
    f3 = is_fill(o3, row)
    f4 = is_fill(o4, row)
    return f1 and f2 and not f3 and not f4


def check_fills_test():
    executions[:] = []
    c1 = len(executions) == 0
    c2 = len(open_orders) == 4
    check_fills(row)
    c3 = len(open_orders) == 2
    c4 = len(executions) == 2
    return c1 and c2 and c3 and c4


print record_fill_test(), ': record_fill_test'
print is_fill_test(), ': is_fill_test'
print check_fills_test(), ': check_fills_test'
