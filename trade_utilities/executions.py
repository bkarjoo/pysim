class Executions(object):
    def __init__(self):
        self.executions = []

    def append(self, execution):
        self.executions.append(execution)

    def shares_traded(self):
        return sum([abs(item.qty) for item in self.executions])

    def __len__(self):
        return len(self.executions)
