class BarDictionary(object):
    def __init__(self):
        self.bars = {}

    def add_row(self, symbol, dt, open, high, low, close, volume):
        if symbol not in self.bars:
            self.bars[symbol] = []
        self.bars[symbol].append((dt, open, high, low, close, volume))

    def get_last_row(self, symbol):
        if symbol in self.bars:
            return self.bars[symbol][-1]
        else:
            return None

    def has_data(self, symbol):
        return symbol in self.bars

    def has_data_today(self, symbol, date):
        c1 = symbol in self.bars
        if not c1: return False
        bar_date = self.bars[symbol][-1][0]
        return bar_date.date() == date.date()

    def get_close_price(self, symbol):
        return self.bars[symbol][-1][4]

    def reset(self):
        self.bars = {}
