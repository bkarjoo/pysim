from trade_utilities.trade import Trade


# groups executions into trades
class Trades(object):
    def __init__(self):
        self.open_trades = []
        self.closed_trades = []
        self.max_exposure = 0.0
        self.max_exposure_long = 0.0
        self.max_exposure_short = 0.0

    def add_execution(self, execution):
        t = self.get_open_trade(execution.symbol)
        if t is None:
            t = Trade()
            self.open_trades.append(t)
        t.add_execution(execution)
        if t.is_closed:
            self.open_trades.remove(t)
            self.closed_trades.append(t)
        self.update_max_exposure()

    def get_open_trade(self, symbol):
        l = [item for item in self.open_trades if item.symbol == symbol]
        if len(l) == 0:
            return None
        elif len(l) == 1:
            return l[0]
        else:
            raise RuntimeError('get_open_trade: Length should never be more than 1 for open trades of a symbol.')

    def get_closed_trade(self, symbol):
        return [item for item in self.closed_trades if item.symbol == symbol]

    def number_of_trades(self):
        return len(self.open_trades) + len(self.closed_trades)

    def number_of_open_trades(self):
        return len(self.open_trades)

    def number_of_closed_trades(self):
        return len(self.closed_trades)

    def number_of_winners(self):
        return len([item for item in self.closed_trades if item.pnl() > 0])

    def number_of_losers(self):
        return len([item for item in self.closed_trades if item.pnl() < 0])

    def win_rate(self):
        return 0 if self.number_of_trades() == 0 else float(self.number_of_winners()) / float(self.number_of_trades())

    def closed_shares(self):
        return sum([item.shares_traded() for item in self.closed_trades])

    def long_shares(self):
        l = [item for item in self.closed_trades if item.side == 'Long']
        return sum([item.shares_traded() for item in l])

    def short_shares(self):
        l = [item for item in self.closed_trades if item.side == 'Short']
        return sum([item.shares_traded() for item in l])

    def open_shares(self):
        return sum([item.shares_traded() for item in self.open_trades])

    def total_shares(self):
        return self.closed_shares() + self.open_shares()

    def closed_shares_long(self):
        l = [item for item in self.closed_trades if item.side == 'Long']
        return sum([item.shares_traded() for item in l])

    def closed_shares_short(self):
        l = [item for item in self.closed_trades if item.side == 'Short']
        return sum([item.shares_traded() for item in l])

    def pnl_closed_trades(self):
        return sum([item.pnl() for item in self.closed_trades])

    def pnl_long_trades(self):
        l = [item for item in self.closed_trades if item.side == 'Long']
        return sum([item.pnl() for item in l])

    def pnl_short_trades(self):
        l = [item for item in self.closed_trades if item.side == 'Short']
        return sum([item.pnl() for item in l])

    def update_last_price(self, symbol, price):
        t = self.get_open_trade(symbol)
        if t is not None:
            t.update_last_price(price)
            self.update_max_exposure()

    def update_max_exposure(self):
        self.max_exposure = max(sum([item.exposure for item in self.open_trades]), self.max_exposure)
        longs = [item for item in self.open_trades if item.side == 'Long']
        shorts = [item for item in self.open_trades if item.side == 'Short']
        self.max_exposure_long = max(sum([item.exposure for item in longs]), self.max_exposure)
        self.max_exposure_short = max(sum([item.exposure for item in shorts]), self.max_exposure_short)

    def biggest_winner(self):
        if len(self.closed_trades) == 0:
            return 0
        return max(0, max([trade.pnl() for trade in self.closed_trades]))

    def biggest_loser(self):
        if len(self.closed_trades) == 0:
            return 0
        return min(0, min([trade.pnl() for trade in self.closed_trades]))

    def cps(self):
        cps = 0 if self.closed_shares() == 0 else self.pnl_closed_trades() / float(self.closed_shares())
        return round(cps * 100, 1)

    def long_cps(self):
        cps = 0 if self.long_shares() == 0 else self.pnl_long_trades() / float(self.long_shares())
        return round(cps * 100, 1)

    def short_cps(self):
        cps = 0 if self.short_shares() == 0 else self.pnl_short_trades() / float(self.short_shares())
        return round(cps * 100, 1)

    def has_position(self, symbol):
        return len([x for x in self.open_trades if x.symbol == symbol]) > 0

    def get_open_position_symbol_list(self):
        return [x.symbol for x in self.open_trades]

    def get_open_position_list(self):
        return [(x.symbol, x.position()) for x in self.open_trades]

    def __str__(self):
        s = ''
        for trade in self.closed_trades:
            s += '{}\n'.format(str(trade))
        for trade in self.open_trades:
            s += '{}\n'.format(str(trade))
        return s

