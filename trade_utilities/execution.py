class Execution(object):
    def __init__(self, qty, symbol, price, date_time):
        self.qty = qty
        self.symbol = symbol
        self.price = price
        self.date_time = date_time

    def __str__(self):
        return '{},{},{},{}'.format(self.date_time, self.qty, self.symbol, self.price)
