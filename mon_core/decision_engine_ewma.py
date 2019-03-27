import numpy as np
from scipy.stats import norm

# convenience class grouping together multiple DEs
class DecisionEngines:

    decision_engines = dict()

    def add_VNF(self, vnf):
        self.decision_engines[vnf] = dict()

    def add_KPI(self, vnf, kpi, init_period, lower_threshold=None, upper_threshold=None, confidence=0.95):
        name = vnf + '_' + kpi
        self.decision_engines[vnf][kpi] = DecisionEngine(name, init_period, lower_threshold, upper_threshold, confidence)

    def feed_data(self, vnf, kpi, value, timestamp=None):
        self.decision_engines[vnf][kpi].feed_data(value, timestamp)

    def get_decision(self, vnf, kpi):
        return self.decision_engines[vnf][kpi].get_decision()


# Decision Engine using exponential smoothing
class DecisionEngine:
    # units in seconds
    mon_periods = [2, 5, 10, 20, 50]

    weight = 0.01

    # thresholds indicate danger levels which we don't want to miss
    # user has to specify at least one of the thresholds
    # 0 < confidence < 1
    def __init__(self, name, init_period, lower_threshold=None, upper_threshold=None, confidence=0.95):
        if lower_threshold is None and upper_threshold is None:
            raise Exception('Specify at least one threshold!')
        self.name = name

        self.initialised = False
        self.ewma = 0.0
        self.ewmv = 0.0
        self.latest_val = 0.0
        self.points_observed = 0

        self.curr_period = init_period

        if lower_threshold is None:
            lower_threshold = -1e10
        if upper_threshold is None:
            upper_threshold = 1e10
        self.ok_interval = (lower_threshold, upper_threshold)
        self.confidence = confidence

    def feed_data(self, value, timestamp=None):
        # assume value stayed the same throughout the period
        if self.points_observed == 0:
            self.ewma = value
            self.ewmv = 0.0

        for i in range(self.curr_period):
            prev_ewma = self.ewma
            self.ewma = (1-self.weight)*self.ewma + self.weight*value
            self.ewmv = (1-self.weight)*self.ewmv + self.weight*(value-self.ewma)*(value-prev_ewma)
        self.latest_val = value
        self.points_observed += 1
        #print('Value = {} EWMA = {} EWMV = {}'.format(value, self.ewma, self.ewmv))

    def get_decision(self):
        # keep default period until we collect more data points
        if self.points_observed < 10:
            return None

        # pick largest period which doesn't cross thresholds with 'confidence' probability
        for period in reversed(self.mon_periods):
            mean = self.latest_val
            std = max(0.01, np.sqrt(self.ewmv*(1+self.weight*(period-1))))
            interval = norm.interval(self.confidence, loc=mean, scale=std)
            #print(mean, std, interval)
            if interval[0] > self.ok_interval[0] and \
               interval[1] < self.ok_interval[1]:
                # period stays unchanged, so no decision to change
                if self.curr_period == period:
                    return None
                self.curr_period = period
                return period
        if self.curr_period == self.mon_periods[0]:
            return None
        self.curr_period = self.mon_periods[0]
        return self.mon_periods[0]


#de = DecisionEngine('aaa', 10, 0, 100, 0.9)
#for i in range(50):
#    val = np.random.normal(80+i/10, 5)
#    de.feed_data(50)
#    #print('EWMA: {}'.format(de.ewma))
#    #print('EWMV: {}'.format(de.ewmv))

#for i in range(900):
#    print('EWMV: {}'.format(de.ewmv))
#    val = np.random.normal(85 - i / 10, 5)
#    de.feed_data(val)
#    d = de.get_decision()
#    if d is None:
#        print('None')
#    else:
#        print(d)