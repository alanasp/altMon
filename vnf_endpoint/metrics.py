import psutil
import os

#Globals
prev_net_usage = 0


def get_net():
    global prev_net_usage
    curr_net_usage = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
    diff = curr_net_usage - prev_net_usage
    prev_net_usage = curr_net_usage
    return diff


def get_ping():
    try:
        ping = os.popen('ping 8.8.8.8 -c1')
        ms = float(ping.readlines()[-1].strip().split('/')[-3])
        return ms
    except:
        return 1e3
