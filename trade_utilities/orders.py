class Orders(object):
    def __init__(self):
        self.open_orders = []

    def reset(self):
        self.open_orders = []

    def __len__(self):
        return len(self.open_orders)

    def append(self, o):
        self.open_orders.append(o)

    def get_open_orders(self, symbol):
        return [o for o in self.open_orders if o.symbol == symbol]

    def remove(self, o):
        self.open_orders.remove(o)

    def has_order(self):
        return len(self.open_orders) > 0

    def pop_order(self):
        return self.open_orders.pop()

