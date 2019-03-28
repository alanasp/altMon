import psutil
import time
from mon_core.decision_engine_ewma import DecisionEngine

metric_functions = {
    'MAX_CPU': lambda: max(psutil.cpu_percent(interval=1, percpu=True)),
    'RAM': lambda: psutil.virtual_memory()[2],
    'Net': lambda: psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
}

mon_periods = {
    'MAX_CPU': 10
}

last_mon_times = {
    'MAX_CPU': -10
}

actual_cpu_file = open('actual_cpu', 'w')
mon_cpu_file = open('mon_cpu', 'w')

bytes_prev = metric_functions['Net']()
time.sleep(1)
seconds_elapsed = 0.0
start_time = time.time()

metric_functions['MAX_CPU']()

cpu_de = DecisionEngine('CPU_DE', mon_periods['MAX_CPU'], upper_threshold=60)

while True:

    bytes_curr = metric_functions['Net']()

    cpu = metric_functions['MAX_CPU']()
    ram = metric_functions['RAM']()
    net = bytes_curr - bytes_prev

    seconds_elapsed = int(time.time() - start_time)

    if seconds_elapsed - last_mon_times['MAX_CPU'] >= mon_periods['MAX_CPU']:
        last_mon_times['MAX_CPU'] = seconds_elapsed
        cpu_de.feed_data(cpu)
        decision = cpu_de.get_decision()
        if decision is not None:
            mon_periods['MAX_CPU'] = decision
        PI = cpu_de.get_PI()
        mon_cpu_file.write('{} {} {} {} {}\n'.format(int(seconds_elapsed), cpu, cpu_de.ewmv, PI[0], PI[1]))
        mon_cpu_file.flush()

    bytes_prev = bytes_curr

    print(cpu, ram, net)
    actual_cpu_file.write('{} {}\n'.format(int(seconds_elapsed), cpu))
    actual_cpu_file.flush()

    time.sleep(1)
