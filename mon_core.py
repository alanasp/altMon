from kafka import KafkaConsumer, KafkaProducer
import psutil
import datetime
import time
import json

from decision_engine import DecisionEngine


def feed_measures(dec_engines, measure_msgs):
    for key in measure_msgs:
        partition = measure_msgs[key]
        for record in partition:
            msg = record.value
            vnf = msg['vnf_name']
            metric = msg['metric']
            value = msg['value']
            timestamp = msg['timestamp']
            dec_engines[vnf][metric].feed_data(value, timestamp)


def get_decision_msgs(dec_engines):
    decision_msgs = list()
    for vnf in dec_engines:
        metric_decisions = dict()
        for metric in dec_engines[vnf]:
            decision = dec_engines[vnf][metric]
            if decision is not None:
                metric_decisions[metric] = dict()
                metric_decisions[metric]['frequency'] = decision
        decision_msgs.append({'vnf_name': vnf, 'metrics': metric_decisions})
    return decision_msgs


# load configurations
with open('mon_core.config', 'r') as config_file:
    config = json.load(config_file)

admin_producer = KafkaProducer(bootstrap_servers=config['bootstrap_servers'],
                               value_serializer=lambda m: json.dumps(m, sort_keys=True).encode('ascii'))

data_consumer = KafkaConsumer(config['data_topic'], bootstrap_servers=config['bootstrap_servers'],
                              value_deserializer=lambda m: json.loads(m.decode('ascii')))

decision_engines = dict()
# create decision engines for all metrics in all VNFs
for vnf in config['VNFs']:
    for metric in config['VNFs'][vnf]['metrics']:
        name = vnf + '_' + metric
        mon_frequency = config['VNFs'][vnf]['metrics'][metric]['frequency']
        decision_engines[vnf] = dict()
        decision_engines[vnf][metric] = DecisionEngine(name, mon_frequency)

while True:
    measures = data_consumer.poll()
    feed_measures(decision_engines, measures)
    admin_msgs = get_decision_msgs(decision_engines)
    for msg in admin_msgs:
        admin_producer.send(config['admin_topic'], msg)
    time.sleep(0.5)
