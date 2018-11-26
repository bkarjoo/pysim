class Orders(object):
    def __init__(self):
        self.open_orders = {}

    def reset(self):
        self.open_orders = {}

    def __len__(self):
        return len(self.open_orders)

    def append(self, o):
        if not o.symbol in self.open_orders:
            self.open_orders[o.symbol] = [o]
        else:
            self.open_orders[o.symbol].append(o)

    def get_open_orders(self, symbol):
        if symbol in self.open_orders:
            return self.open_orders[symbol]
        else:
            return None

    def remove(self, o):
        self.open_orders.remove(o)

    def keys(self):
        return self.open_orders.keys()

