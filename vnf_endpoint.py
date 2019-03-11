from kafka import KafkaConsumer, KafkaProducer
import psutil
import datetime
import time
import json

# functions called to get current values of metrics
# should be filled with all values in available in config
metric_functions = {
    'CPU': lambda: psutil.cpu_percent(),
    'RAM': lambda: psutil.virtual_memory()[2]
}


# admin messages expected in JSON format
# pick most recent message addressed to this VNF and update the respective configurations
def update_config(current_config, update_msgs):
    for key in update_msgs:
        partition = update_msgs[key]
        for record in partition:
            msg = json.loads(record.value)
            print(msg)
            if msg['vnf_name'] == current_config['vnf_name']:
                current_config['metrics'].update(msg['metrics'])
                return


# load configurations
with open('vnf_endpoint.config', 'r') as config_file:
    config = json.load(config_file)

admin_consumer = KafkaConsumer(config['admin_topic'], bootstrap_servers=config['bootstrap_servers'])

data_producer = KafkaProducer(bootstrap_servers=config['bootstrap_servers'])

last_mon_times = dict()

now = datetime.datetime.utcnow()
print(now)
for metric in config['metrics']:
    last_mon_times[metric] = now

while True:
    admin_msgs = admin_consumer.poll()
    update_config(config, admin_msgs)
    for metric in last_mon_times:
        now = datetime.datetime.utcnow()
        delta = (now - last_mon_times[metric]).seconds
        if delta >= config['metrics'][metric]['frequency']:
            value = metric_functions[metric]()
            msg = {'timestamp': now.isoformat(), 'vnf_name': config['vnf_name'], 'metric': metric, 'value': value}
            data_producer.send(config['data_topic'], json.dumps(msg))
            last_mon_times[metric] = now
    time.sleep(0.5)
