class Order(object):
    def __init__(self, qty, symbol, price, is_opg = False):
        self.qty = qty
        self.symbol = symbol
        self.price = price
        self.is_opg = is_opg
