import numpy as np
from scipy.stats import norm

# convenience class grouping together multiple DEs
class DecisionEngines:

    decision_engines = dict()

    # indicates order of taking measurements
    tiers = dict()

    # monitor all tiers up to and including this (e.g.: active_tier = 2 then monitor tiers 0, 1 and 2)
    active_tier = 0

    highest_tier = 0

    def add_VNF(self, vnf):
        self.decision_engines[vnf] = dict()
        self.tiers[vnf] = dict()

    def add_KPI(self, vnf, kpi, init_period, lower_threshold=None, upper_threshold=None, confidence=0.95, tier=0):
        name = vnf + '_' + kpi
        self.decision_engines[vnf][kpi] = DecisionEngine(name, init_period, lower_threshold, upper_threshold, confidence)
        self.tiers[vnf][kpi] = tier
        self.highest_tier = max(tier, self.highest_tier)

    def get_PI(self, vnf, metric):
        return self.decision_engines[vnf][metric].get_PI()

    def is_active(self, vnf, kpi):
        if vnf not in self.tiers or kpi not in self.tiers[vnf]:
            return False
        return self.get_tier(vnf, kpi) <= self.active_tier

    def get_tier(self, vnf, kpi):
        return self.tiers[vnf][kpi]

    def deactivate_above_active(self):
        for vnf in self.tiers:
            for kpi in self.tiers[vnf]:
                if self.tiers[vnf][kpi] > self.active_tier:
                    self.cut_off(vnf, kpi)

    def cut_off(self, vnf, metric):
        self.decision_engines[vnf][metric].cut_off()

    def feed_data(self, vnf, kpi, value, timestamp=None):
        self.decision_engines[vnf][kpi].feed_data(value, timestamp)

    def get_decision(self, vnf, kpi):
        return self.decision_engines[vnf][kpi].get_decision()

    def get_curr_period(self, vnf, kpi):
        return self.decision_engines[vnf][kpi].curr_period

    def is_violated(self, vnf, kpi):
        return self.decision_engines[vnf][kpi].is_violated()

    def increase_tier(self):
        # can't go above highest
        if self.active_tier < self.highest_tier:
            self.active_tier += 1
            return True
        return False

    def decrease_tier(self):
        # can't go below 0
        if self.active_tier > 0:
            self.active_tier -= 1
            return True
        return False



# Decision Engine using exponential smoothing
class DecisionEngine:
    # units in seconds
    mon_periods = [2, 5, 10, 20, 50]

    weight = 0.03

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

        # indicates if any data was fed into the system before requesting another decision
        self.fed_data = False

        self.curr_period = init_period

        if lower_threshold is None:
            lower_threshold = -1e10
        if upper_threshold is None:
            upper_threshold = 1e10
        self.ok_interval = (lower_threshold, upper_threshold)
        self.confidence = confidence

    # indicate that data flow will stop, so will have to go through the initial phase again
    def cut_off(self):
        self.points_observed = 0

    def is_violated(self):
        if self.points_observed == 0:
            return False
        return self.latest_val < self.ok_interval[0] or self.latest_val > self.ok_interval[1]

    # function used for exporting current PIs
    def get_PI(self):
        mean = self.latest_val
        std = max(0.01, np.sqrt(self.ewmv * (1 + self.weight * (self.curr_period - 1))))
        interval = norm.interval(self.confidence, loc=mean, scale=std)
        return interval

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
        self.fed_data = True
        #print('Value = {} EWMA = {} EWMV = {}'.format(value, self.ewma, self.ewmv))

    def get_decision(self):
        if not self.fed_data:
            return None
        # keep default period until we collect more data points
        if self.points_observed < 10:
            return None
        #can at most increase monitoring period to the next one in the list
        curr_index = self.mon_periods.index(self.curr_period)
        max_index = min(curr_index+1, len(self.mon_periods)-1)

        # pick largest period which doesn't cross thresholds with 'confidence' probability
        for i in reversed(range(max_index+1)):
            period = self.mon_periods[i]
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

