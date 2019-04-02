import numpy as np
import matplotlib.pyplot as plt
import math
from matplotlib.ticker import MaxNLocator
from mpl_toolkits.mplot3d import Axes3D
import datetime as dt

plt_color_codes = 'bgrcmykw'

exp_file_dir = 'saved_exp_data/5/'

time_start = dt.datetime.strptime('2019-03-30T04:48:25', '%Y-%m-%dT%H:%M:%S').time()
time_end = dt.datetime.strptime('2019-03-30T04:56:43', '%Y-%m-%dT%H:%M:%S').time()

ping_vals = list()
times = list()

thresh = 20

tier1_start = None
tier1_end = None

filename = exp_file_dir + 'eNodeB_ping'
with open(filename, 'r') as file:
    for line in file:
        time, value = line.split(' ')
        time = dt.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%f').time()
        value = float(value)
        if time < time_start:
            continue
        if time > time_end:
            break
        diff = (dt.datetime.combine(dt.date.today(), time) - dt.datetime.combine(dt.date.today(), time_start)).total_seconds()
        if value > thresh and tier1_start is None:
            tier1_start = diff
        elif value < thresh and tier1_end is None:
            tier1_end = diff
        if value > thresh:
            tier1_end = None
        times.append(diff)
        ping_vals.append(value)

print(ping_vals)
print(times)

fig, ax = plt.subplots()


plt_tier1 = ax.axvspan(tier1_start, tier1_end, alpha=0.2, color='y', label='Active tier = 1')
plt_tier0 = ax.axvspan(0, tier1_start, alpha=0.2, color='g', label='Active tier = 0')
ax.axvspan(tier1_end, 500, alpha=0.2, color='g', label='Active tier = 0')

plt_danger = plt.fill_between([0, 500], [thresh, thresh], [35, 35], color='r', label='Danger Zone', alpha=0.5)

plt_ping_vals = plt.scatter(times, ping_vals, zorder=1, s=10, label='Observed ping values [ms]')

plt.legend(handles=[plt_danger, plt_ping_vals, plt_tier0, plt_tier1], fontsize=10)

plt.xlabel('Time [s]', fontsize=10)
plt.ylabel('Ping Google DNS [ms]', fontsize=10)
axes = plt.gca()
axes.set_ylim(0, 35)
axes.set_xlim(0, 500)


plt.show()
