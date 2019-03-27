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

first_admin_received = False

# admin messages expected in JSON format
# pick most recent message addressed to this VNF and update the respective configurations
def update_config(current_config, update_msgs):
    for key in update_msgs:
        partition = update_msgs[key]
        for record in partition:
            first_admin_received = True
            msg = record.value
            if msg['vnf_name'] == current_config['vnf_name']:
                current_config['metrics'].update(msg['metrics'])
                with open('vnf_endpoint.config', 'w') as config_file:
                    json.dump(current_config, config_file, sort_keys=True)
                return


# load configurations
with open('vnf_endpoint.config', 'r') as config_file:
    config = json.load(config_file)

admin_consumer = KafkaConsumer(config['ext_admin_topic'], bootstrap_servers=config['kafka_ext'],
                               value_deserializer=lambda m: json.loads(m.decode('ascii')))

data_producer = KafkaProducer(bootstrap_servers=config['kafka_ext'],
                              value_serializer=lambda m: json.dumps(m, sort_keys=True).encode('ascii'))

last_mon_times = dict()

now = datetime.datetime.utcnow()
for metric in config['metrics']:
    last_mon_times[metric] = now

while True:
    admin_msgs = admin_consumer.poll()
    update_config(config, admin_msgs)
    if first_admin_received:
        with open('actual_CPU', 'a+') as cpu_file:
            cpu_file.write(str(metric_functions['CPU']()))

    for metric in last_mon_times:
        now = datetime.datetime.utcnow()
        delta = (now - last_mon_times[metric]).seconds
        if delta >= config['metrics'][metric]['mon_period']:
            value = metric_functions[metric]()
            msg = {'timestamp': now.isoformat(), 'vnf_name': config['vnf_name'], 'metric': metric, 'value': value}
            data_producer.send(config['ext_data_topic'], msg)
            last_mon_times[metric] = now
    time.sleep(1)
