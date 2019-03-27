from kafka import KafkaConsumer, KafkaProducer
import psutil
import datetime
import time
import json

from decision_engine_ewma import DecisionEngines


def feed_measures(dec_engines, measure_msgs, export, export_producer, export_topic):
    for key in measure_msgs:
        partition = measure_msgs[key]
        for record in partition:
            msg = record.value
            print('Message received: {}'.format(msg))
            if export:
                export_producer.send(export_topic, msg)
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


def get_export_status(osm_export_msgs):
    returned_status = None
    for key in osm_export_msgs:
        partition = osm_export_msgs[key]
        for record in partition:
            msg = record.value
            if 'export_kind' in msg:
                if msg['export_kind'] == 'full-export':
                    return True
                elif msg['export_kind'] == 'no-export':
                    return False

    return returned_status


print('Loading configurations...')
# load configurations
with open('mon_core.config', 'r') as config_file:
    config = json.load(config_file)

print('Configurations loaded!')

print('Connecting to external Kafka cluster at {}...'.format(config['kafka_ext']))

admin_producer = KafkaProducer(bootstrap_servers=config['kafka_ext'],
                               value_serializer=lambda m: json.dumps(m, sort_keys=True).encode('ascii'))

data_consumer = KafkaConsumer(config['ext_data_topic'], bootstrap_servers=config['kafka_ext'],
                              value_deserializer=lambda m: json.loads(m.decode('ascii')))

print('Connected to external Kafka cluster')

print('Connecting to OSM Kafka cluster at {}...'.format(config['kafka_osm']))

osm_producer = KafkaProducer(bootstrap_servers=config['kafka_osm'],
                             value_serializer=lambda m: json.dumps(m, sort_keys=True).encode('ascii'))

osm_consumer = KafkaConsumer(config['osm_mon_config_topic'], bootstrap_servers=config['kafka_osm'],
                             value_deserializer=lambda m: json.loads(m.decode('ascii')))

print('Connected to OSM Kafka cluster')

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
    admin_producer.send(config['ext_admin_topic'], msg)

export_mon_data = False

while True:
    osm_msgs = osm_consumer.poll()
    export_status = get_export_status(osm_msgs)
    if export_status is not None:
        export_mon_data = export_status

    measures = data_consumer.poll()
    feed_measures(DEs, measures, export_mon_data, osm_producer, config['osm_mon_export_topic'])
    admin_msgs = get_decision_msgs(DEs, config)
    for msg in admin_msgs:
        print('Sending message: {}'.format(msg))
        if export_mon_data:
            osm_producer.send(config['osm_mon_export_topic'], msg)
        admin_producer.send(config['ext_admin_topic'], msg)
    time.sleep(0.5)
