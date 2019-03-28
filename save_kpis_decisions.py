import psutil
import time
from mon_core.decision_engine_ewma import DecisionEngine

metric_functions = {
    'CPU': lambda: psutil.cpu_percent(),
    'RAM': lambda: psutil.virtual_memory()[2],
    'Net': lambda: psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
}

mon_periods = {
    'CPU': 10
}

last_mon_times = {
    'CPU': -10
}

actual_cpu_file = open('actual_cpu', 'w')
mon_cpu_file = open('mon_cpu', 'w')

bytes_prev = metric_functions['Net']()
time.sleep(1)
seconds_elapsed = 0

cpu_de = DecisionEngine('CPU_DE', mon_periods['CPU'], upper_threshold=60)

while True:

    bytes_curr = metric_functions['Net']()

    cpu = metric_functions['CPU']()
    ram = metric_functions['RAM']()
    net = bytes_curr - bytes_prev

    if seconds_elapsed - last_mon_times['CPU'] >= mon_periods['CPU']:
        last_mon_times['CPU'] = seconds_elapsed
        cpu_de.feed_data(cpu)
        decision = cpu_de.get_decision()
        if decision is not None:
            mon_periods['CPU'] = decision
        PI = cpu_de.get_PI()
        mon_cpu_file.write('{} {} {} {} {}\n'.format(seconds_elapsed, cpu, cpu_de.ewmv, PI[0], PI[1]))
        mon_cpu_file.flush()

    bytes_prev = bytes_curr

    print(cpu, ram, net)
    actual_cpu_file.write('{} {}\n'.format(seconds_elapsed, cpu))
    actual_cpu_file.flush()

    seconds_elapsed += 1
    time.sleep(1)
