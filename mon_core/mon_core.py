from kafka import KafkaConsumer, KafkaProducer
import psutil
import datetime
import time
import json

from decision_engine_ewma import DecisionEngines


def feed_measures(dec_engines, measure_msgs):
    for key in measure_msgs:
        partition = measure_msgs[key]
        for record in partition:
            msg = record.value
            print('Message received: {}'.format(msg))
            vnf = msg['vnf_name']
            metric = msg['metric']
            value = msg['value']
            timestamp = msg['timestamp']
            dec_engines.feed_data(vnf, metric, value, timestamp)


def get_decision_msgs(dec_engines, config_dict, init=False):
    decision_msgs = list()
    for vnf in config_dict['VNFs']:
        metric_decisions = dict()
        if init:
            metric_decisions = config_dict['VNFs'][vnf]['metrics']
        else:
            for metric in config_dict['VNFs'][vnf]['metrics']:
                decision = dec_engines.get_decision(vnf, metric)
                if decision is not None:
                    metric_decisions[metric] = dict()
                    metric_decisions[metric]['mon_period'] = decision
        if len(metric_decisions) > 0:
            decision_msgs.append({'vnf_name': vnf, 'metrics': metric_decisions})
    return decision_msgs


print('Loading configurations...')
# load configurations
with open('mon_core.config', 'r') as config_file:
    config = json.load(config_file)

print('Configurations loaded!')

print('Connecting to Kafka cluster...')

admin_producer = KafkaProducer(bootstrap_servers=config['bootstrap_servers'],
                               value_serializer=lambda m: json.dumps(m, sort_keys=True).encode('ascii'))

data_consumer = KafkaConsumer(config['data_topic'], bootstrap_servers=config['bootstrap_servers'],
                              value_deserializer=lambda m: json.loads(m.decode('ascii')))

print('Connected to Kafka cluster')

DEs = DecisionEngines()
# create decision engines for all metrics in all VNFs
for vnf in config['VNFs']:
    DEs.add_VNF(vnf)
    for metric in config['VNFs'][vnf]['metrics']:
        mon_period = config['VNFs'][vnf]['metrics'][metric]['mon_period']
        lower_threshold = None
        if 'lower_threshold' in config['VNFs'][vnf]['metrics'][metric]:
            lower_threshold = config['VNFs'][vnf]['metrics'][metric]['lower_threshold']
        upper_threshold = None
        if 'upper_threshold' in config['VNFs'][vnf]['metrics'][metric]:
            upper_threshold = config['VNFs'][vnf]['metrics'][metric]['upper_threshold']
        DEs.add_KPI(vnf, metric, mon_period, lower_threshold, upper_threshold)

# send initial monitoring periods
init_admin_msgs = get_decision_msgs(DEs, config, init=True)
for msg in init_admin_msgs:
    print('Sending message: {}'.format(msg))
    admin_producer.send(config['admin_topic'], msg)

while True:
    measures = data_consumer.poll()
    feed_measures(DEs, measures)
    admin_msgs = get_decision_msgs(DEs, config)
    for msg in admin_msgs:
        print('Sending message: {}'.format(msg))
        admin_producer.send(config['admin_topic'], msg)
    time.sleep(0.5)
