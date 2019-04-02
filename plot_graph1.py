import numpy as np
import matplotlib.pyplot as plt
import math
from matplotlib.ticker import MaxNLocator
from mpl_toolkits.mplot3d import Axes3D

plt_color_codes = 'bgrcmykw'

mon_filename = 'saved_exp_data/mon_cpu_1'
actual_filename = 'saved_exp_data/actual_cpu_1'

times = list()
vals = list()
stds = list()
lows = list()
highs = list()

with open(mon_filename, 'r') as mon_file:
    for line in mon_file:
        time, val, ewmv, low, high = map(float, line.split(' '))
        print(time, val, ewmv, low, high)
        times.append(time)
        vals.append(val)
        stds.append(math.sqrt(ewmv))
        lows.append(max(low, 0))
        highs.append(min(high, 100))

all_times = list()
all_vals = list()

with open(actual_filename, 'r') as actual_file:
    for line in actual_file:
        time, val = map(float, line.split(' '))
        all_times.append(time)
        all_vals.append(val)

plt_danger = plt.fill_between([0, 1000], [60, 60], [100, 100], color='r', label='Danger Zone', alpha=0.2)
plt_all, = plt.plot(all_times, all_vals, zorder=-1, label='Actual values at all times')
plt_pi = plt.fill_between(times, lows, highs, color='g', label='PI of current monitoring period for confidence=0.95', alpha=0.2)
plt_observed = plt.scatter(times, vals, color='r', s=16, zorder=1, label='Observed values')

plt.legend(handles=[plt_danger, plt_all, plt_pi, plt_observed], fontsize=14)

plt.xlabel('Time [s]', fontsize=20)
plt.ylabel('Max(CPU core utilizations) [%]', fontsize=20)
axes = plt.gca()
axes.set_ylim(0, 100)
axes.set_xlim(0, 1000)


plt.show()
