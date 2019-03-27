import psutil
import time

metric_functions = {
    'CPU': lambda: psutil.cpu_percent(),
    'RAM': lambda: psutil.virtual_memory()[2],
    'Net': lambda: psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
}

file = open('measures', 'w')

bytes_prev = metric_functions['Net']()
time.sleep(1)

while True:

    bytes_curr = metric_functions['Net']()

    cpu = metric_functions['CPU']()
    ram = metric_functions['RAM']()
    net = bytes_curr - bytes_prev

    bytes_prev = bytes_curr

    print(cpu, ram, net)
    file.write('{} {} {}\n'.format(cpu, ram, net))
    file.flush()

    time.sleep(1)
