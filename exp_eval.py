import numpy as np
import datetime as dt

exp_file_dir = 'saved_exp_data/5/'

vnfs = ['eNodeB', 'HSS', 'MME', 'SPGW']
kpis = ['CPU', 'RAM', 'Net', 'ping']

avg_vals = dict()
std_vals = dict()

time_start = dt.datetime.strptime('2019-03-30T04:48:25', '%Y-%m-%dT%H:%M:%S').time()
time_end = dt.datetime.strptime('2019-03-30T04:56:43', '%Y-%m-%dT%H:%M:%S').time()

for vnf in vnfs:
    for kpi in kpis:
        if kpi == 'ping' and vnf != 'eNodeB':
            continue
        vals = list()
        filename = exp_file_dir + vnf + '_' + kpi
        with open(filename, 'r') as file:
            for line in file:
                time, value = line.split(' ')
                time = dt.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%f').time()
                value = float(value)
                if kpi == 'Net' and vnf == 'eNodeB':
                    value /= 10*1e4
                elif kpi == 'Net':
                    value /= 50*1e4
                if time < time_start:
                    continue
                if time > time_end:
                    break
                vals.append(value)
        avg_vals[vnf + '_' + kpi] = np.mean(vals)
        std_vals[vnf + '_' + kpi] = np.std(vals)

print('Average, std')
for key in avg_vals:
    print('{}: {:.3f} {:.3f}'.format(key, avg_vals[key], std_vals[key]))
