from kibot.kibot_downloader import get_close


class DailyReturn(object):
    def __init__(self):
        self.previous_date = None
        self.today_date = None
        self.transactions = []

    def reset(self, previous_date, today_date, positions):
        self.previous_date = previous_date
        self.today_date = today_date
        self.transactions = []
        self.open_positions(positions)

    def add_opening_position(self, position_qty, price):
        self.transactions.append(price * position_qty * -1)

    def add_closing_position(self, position_qty, price):
        self.transactions.append(price * position_qty)

    def add_transaction(self, qty, price):
        self.transactions.append(price * qty * -1)

    def open_positions(self, positions):
        for pos in positions:
            # get the close price
            close_price = get_close(pos[0], self.previous_date)
            self.add_opening_position(pos[1], close_price)

    def close_positions(self, positions):
        for symbol, pos in positions:
            # get the close price
            close_price = get_close(symbol, self.today_date)
            self.add_closing_position(pos, close_price)

    def get_transactions_sum(self):
        return sum(self.transactions)
