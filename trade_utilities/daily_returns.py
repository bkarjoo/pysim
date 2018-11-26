import pandas as pd

class DailyReturns(object):
    def __init__(self):
        self.returns = []

    # takes a daily return object and appends it to list
    def append(self, daily_return):
        self.returns.append((daily_return.today_date, daily_return.get_transactions_sum()))

    def gross(self):
        return sum([pair[1] for pair in self.returns])

    def net(self, shares, commissions):
        return sum([pair[1] for pair in self.returns]) - (shares * commissions)
