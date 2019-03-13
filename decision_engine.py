import numpy as np


# only retains the latest elements added up to set size
class SlidingWindow:
    def __init__(self, size):
        self.data = []
        self.current = 0
        self.size = size

    def append(self, item):
        pass


# dummy implementation
class DecisionEngine:
    mon_frequencies = [2, 5, 10, 20, 50]

    def __init__(self, name, init_frequency):
        self.name = name
        self.data = list()
        self.curr_frequency = init_frequency

    def feed_data(self, value, timestamp):
        self.data.append((value, timestamp))

    def get_decision(self):
        rand = np.random.randint(5)
        keep_curr = np.random.uniform()
        # None decision indicates no change to current monitoring frequency
        if keep_curr < 0.7:
            return None
        return self.mon_frequencies[rand]
