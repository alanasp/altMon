from kafka import KafkaConsumer, KafkaProducer
import psutil
import time
import json

metric_functions = {
    'CPU': lambda: psutil.cpu_percent(),
    'RAM': lambda: psutil.virtual_memory()[2]
}

with open('vnf_endpoint.config', 'r') as config_file:
    config = json.load(config_file)

admin_consumer = KafkaConsumer('altmon-admin', bootstrap_servers=['10.10.20.137:9092'])

data_producer = KafkaProducer(bootstrap_servers=['10.10.20.137:9092'])

last_mon_times = dict()

now = time.gmtime()
print(now)
for metric in config['metrics']:
    last_mon_times[metric] = now

while True:
    now = time.gmtime()
    admin_msgs = admin_consumer.poll()
    print(admin_msgs)
    print(metric_functions['CPU']())
    print(metric_functions['RAM']())
    time.sleep(0.5)
