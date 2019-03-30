from kafka import KafkaConsumer, KafkaProducer
import psutil
import datetime
import time
import json
import sys

import metrics


# Globals
first_admin_received = False
config = dict()
config_filename = 'vnf_endpoint.config'

if len(sys.argv) > 0:
    config_filename = 'vnf_endpoint.' + sys.argv[1] + '.config'

# functions called to get current values of metrics
# should be filled with all values in available in config
metric_functions = {
    'CPU': lambda: psutil.cpu_percent(),
    'RAM': lambda: psutil.virtual_memory()[2],
    'Net': lambda: metrics.get_net() / config['metrics'][metric]['mon_period'],
    'ping': lambda: metrics.get_ping()
}


# admin messages expected in JSON format
# pick most recent message addressed to this VNF and update the respective configurations
def update_config(current_config, update_msgs):
    for key in update_msgs:
        partition = update_msgs[key]
        for record in partition:
            first_admin_received = True
            msg = record.value
            print('Message received: {}'.format(msg))
            if msg['vnf_name'] == current_config['vnf_name']:
                current_config['metrics'].update(msg['metrics'])
                with open(config_filename, 'w') as config_file:
                    json.dump(current_config, config_file, sort_keys=True, indent=2)
                return


print('Loading configurations...')

# load configurations
with open(config_filename, 'r') as config_file:
    config = json.load(config_file)

print('Configurations loaded!')

print('Connecting to external Kafka cluster at {}...'.format(config['kafka_ext']))

admin_consumer = KafkaConsumer(config['ext_admin_topic'], bootstrap_servers=config['kafka_ext'],
                               value_deserializer=lambda m: json.loads(m.decode('ascii')))

data_producer = KafkaProducer(bootstrap_servers=config['kafka_ext'],
                              value_serializer=lambda m: json.dumps(m, sort_keys=True).encode('ascii'))

print('Connected to external Kafka cluster')

last_mon_times = dict()

now = datetime.datetime.utcnow()
for metric in config['metrics']:
    last_mon_times[metric] = now
    # first run for initialization
    metric_functions[metric]()

while True:


    admin_msgs = admin_consumer.poll()
    update_config(config, admin_msgs)

    for metric in last_mon_times:
        now = datetime.datetime.utcnow()
        delta = (now - last_mon_times[metric]).seconds
        if delta >= config['metrics'][metric]['mon_period']:
            value = metric_functions[metric]()
            msg = {'timestamp': now.isoformat(), 'vnf_name': config['vnf_name'], 'metric': metric, 'value': value}
            print('Sending message: {}'.format(msg))
            data_producer.send(config['ext_data_topic'], msg)
            last_mon_times[metric] = now
    time.sleep(0.1)
