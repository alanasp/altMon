import psutil


#Globals
prev_net_usage = 0


def get_net():
    global prev_net_usage
    curr_net_usage = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
    diff = curr_net_usage - prev_net_usage
    prev_net_usage = curr_net_usage
    return diff
