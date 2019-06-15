import numpy as np
from scipy.stats import norm
import pmdarima as pm

import matplotlib.pyplot as plt

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

    # thresholds indicate danger levels which we don't want to miss
    # user has to specify at least one of the thresholds
    # 0 < confidence < 1
    def __init__(self, name, init_period, lower_threshold=None, upper_threshold=None, confidence=0.95):
        if lower_threshold is None and upper_threshold is None:
            raise Exception('Specify at least one threshold!')
        self.name = name

        self.initialised = False

        self.series = list()

        self.arima = None
        self.fitted_model = None
        self.curr_interval = None

        # indicates if any data was fed into the system before requesting another decision
        self.fed_data = False

        self.curr_period = init_period

        if lower_threshold is None:
            lower_threshold = -1e10
        if upper_threshold is None:
            upper_threshold = 1e10
        self.ok_interval = (lower_threshold, upper_threshold)
        self.confidence = confidence
        self.alpha = 1 - confidence

    # indicate that data flow will stop, so will have to go through the initial phase again
    def cut_off(self):
        self.series = list()

    def is_violated(self):
        if len(self.series) == 0:
            return False
        return self.series[-1] < self.ok_interval[0] or self.series[-1] > self.ok_interval[1]

    # function used for exporting current PIs
    def get_PI(self):
        return self.curr_interval

    def feed_data(self, value, timestamp=None):
        # assume value stayed the same throughout the period
        if self.fitted_model is not None:
            forecasts, intervals = self.fitted_model.predict(n_periods=self.curr_period-1,
                                                             return_conf_int=True, alpha=self.alpha)
            self.fitted_model.update(forecasts + [value])
            for forecast in forecasts:
                self.series.append(forecast)

        else:
            for i in range(1, self.curr_period):
                self.series.append(value + np.random.normal())

        self.series.append(value)
        self.fed_data = True

    def get_decision(self):
        if not self.fed_data:
            return None
        self.fed_data = False

        # keep default period until we collect more data points
        if len(self.series) < 100:
            return None

        if self.arima is None or len(self.series) % 10000 == 0:
            self.arima = pm.auto_arima(self.series, start_p=0, start_q=0, max_p=0, max_d=1, max_q=0, seasonal=False)
            self.fitted_model = self.arima.fit(self.series)
            print('ARIMA: {}'.format(self.arima))
            print('Fitted order: {}'.format(self.arima.order))

        #print(self.series[-1])
        forecasts, intervals = self.fitted_model.predict(n_periods=self.mon_periods[-1],
                                                         return_conf_int=True, alpha=self.alpha)
        #print(forecasts)
        #print(intervals)
        max_period = -1
        # pick largest period which doesn't cross thresholds with 'confidence' probability
        for i in range(len(intervals)):
            interval = intervals[i]
            if interval[0] < self.ok_interval[0] or \
               interval[1] > self.ok_interval[1]:
                max_period = i - 1
                break
        period_index = 0
        for i in reversed(range(len(self.mon_periods))):
            if self.mon_periods[i] <= max_period:
                period_index = i
                break

        self.curr_interval = intervals[self.mon_periods[period_index]]
        if self.curr_period == self.mon_periods[period_index]:
            return None
        self.curr_period = self.mon_periods[period_index]
        return self.mon_periods[period_index]

"""
de = DecisionEngine('DE1', 10, 0, 100, 0.9)
li = list()
for i in range(500):
    val = np.random.normal(80+i/10, 5)
    de.feed_data(val)
    #li.append(val)
print(de.series)
#results = pm.auto_arima(de.series, trace=True)

#exit(0)

for i in range(1):
    val = np.random.normal(130, 5)
    de.feed_data(val)
    print(i)
    d = de.get_decision()
    if d is None:
        print('None')
    else:
        print(d)

x_obs = np.linspace(1, len(de.series), num=len(de.series))
y_obs = de.series

n_periods = 1000
x_forecast = np.linspace(len(de.series)+1, len(de.series) + n_periods, num=n_periods)
y_forecast, intervals = de.fitted_model.predict(n_periods=n_periods, return_conf_int=True, alpha=de.alpha)
low = intervals[:, 0]
high = intervals[:, 1]

plt_pi = plt.fill_between(x_forecast, low, high, color='g', label='PI for confidence=0.95', alpha=0.2)

plt.plot(x_obs, y_obs, x_forecast, y_forecast)
plt.show()
"""

