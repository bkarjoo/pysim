class Trade(object):
    def __init__(self):
        self.executions = []
        self.symbol = ''
        self.is_closed = False
        self.side = None
        self.last_price = 0.0
        # update max_exposure on execution and last_price
        self.exposure = 0.0
        self.open_date = None
        self.close_date = None

    def add_execution(self, execution):
        if self.open_date is None:
            self.open_date = execution.date_time
        if self.symbol == '':
            self.symbol = execution.symbol
        if self.side is None:
            if execution.qty > 0:
                self.side = 'Long'
            elif execution.qty < 0:
                self.side = 'Short'
            else:
                raise RuntimeError('0 qty execution is being added to Trade object.')
        if self.symbol != execution.symbol:
            raise RuntimeError('Execution symbol not the same as trade symbol.')
        if self.is_closed:
            raise ValueError
        self.executions.append(execution)
        self.update_last_price(execution.price)
        if self.position() == 0:
            self.close_date = execution.date_time
            self.is_closed = True

    def position(self):
        return sum([item.qty for item in self.executions])

    def shares_traded(self):
        return sum([abs(item.qty) for item in self.executions])

    def position_status(self):
        if self.is_closed:
            return 'Closed'
        else:
            return 'Open'

    def pnl(self):
        return sum([(item.qty * item.price * -1) for item in self.executions])

    def update_last_price(self, price):
        self.last_price = price
        self.update_exposure()

    def update_exposure(self):
        self.exposure = abs(self.position()) * self.last_price

    def average_entry_price(self):
        total_qty = 0
        total_value = 0
        for e in self.executions:
            if self.side == 'Long':
                if e.qty > 0:
                    total_qty += e.qty
                    total_value += e.qty * e.price
            elif self.side == 'Short':
                if e.qty < 0:
                    total_qty += e.qty
                    total_value += e.qty * e.price
            else:
                return 0
        return total_value / float(total_qty)

    def average_exit_price(self):
        total_qty = 0
        total_value = 0
        for e in self.executions:
            if self.side == 'Long':
                if e.qty < 0:
                    total_qty += e.qty
                    total_value += e.qty * e.price
            elif self.side == 'Short':
                if e.qty > 0:
                    total_qty += e.qty
                    total_value += e.qty * e.price
            else:
                return 0
        return total_value / float(total_qty)

    def __str__(self):
        s = '{},{},{},{},{},{},{}'.format(self.open_date, self.side, self.shares_traded(), self.symbol,
                                          self.average_entry_price(), self.average_exit_price(), self.pnl())
        return s
