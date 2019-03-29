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
                timestamp = msg['timestamp']
                with open('{}_{}'.format(vnf, metric), 'a+') as results_file:
                    results_file.write(str(timestamp) + ' ' + str(value) + '\n')


print('Loading configurations...')
# load configurations
with open('mon_consumer_ext.config', 'r') as config_file:
    config = json.load(config_file)

print('Configurations loaded!')

print('Connecting to External Kafka cluster at {}...'.format(config['kafka_ext']))

data_consumer = KafkaConsumer(config['ext_data_topic'], bootstrap_servers=config['kafka_ext'],
                              value_deserializer=lambda m: json.loads(m.decode('ascii')))

print('Connected to Kafka cluster')


while True:
    measures = data_consumer.poll()
    save_measures(measures)
    time.sleep(1)
