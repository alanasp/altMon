from kafka import KafkaConsumer, KafkaProducer
import datetime
import time
import json


def save_measures(measure_msgs):
    for key in measure_msgs:
        partition = measure_msgs[key]
        for record in partition:
            msg = record.value
            print('Message received: {}'.format(msg))
            if 'value' in msg:
                vnf = msg['vnf_name']
                metric = msg['metric']
                value = msg['value']
                mon_period = msg['mon_period']
                PI = msg['prediction_interval']
                timestamp = msg['timestamp']
                with open('{}_{}'.format(vnf, metric), 'a+') as results_file:
                    results_file.write(str(value) + ' ' + str(mon_period) + ' ' + str(PI[0]) + ' ' + str(PI[1]))


print('Loading configurations...')
# load configurations
with open('mon_consumer.config', 'r') as config_file:
    config = json.load(config_file)

print('Configurations loaded!')

print('Connecting to OSM Kafka cluster at {}...'.format(config['kafka_osm']))

config_producer = KafkaProducer(bootstrap_servers=config['kafka_osm'],
                                value_serializer=lambda m: json.dumps(m, sort_keys=True).encode('ascii'))

data_consumer = KafkaConsumer(config['osm_mon_export_topic'], bootstrap_servers=config['kafka_osm'],
                              value_deserializer=lambda m: json.loads(m.decode('ascii')))

print('Connected to Kafka cluster')

export_kind = ''
allowed_kinds = config['allowed_export_kinds']
while export_kind not in allowed_kinds:
    export_kind = input('Enter export kind (one of {}): '.format(allowed_kinds))

config_producer.send(config['osm_mon_config_topic'], {'export_kind': export_kind})

while True:
    measures = data_consumer.poll()
    save_measures(measures)
    time.sleep(1)
