from kafka import KafkaConsumer, KafkaProducer
import psutil
import datetime
import time
import json

from decision_engine_ewma import DecisionEngines

# Globals
export_to_file = True
osm_export = False


def feed_measures(dec_engines, measure_msgs, export=False, export_producer=None, export_topic=None):
    for key in measure_msgs:
        partition = measure_msgs[key]
        for record in partition:
            msg = record.value
            print('Message received: {}'.format(msg))
            vnf = msg['vnf_name']
            metric = msg['metric']
            value = msg['value']
            timestamp = msg['timestamp']
            if metric == 'Net':
                print(dec_engines.decision_engines[vnf][metric].get_PI())
            if dec_engines.is_active(vnf, metric):
                dec_engines.feed_data(vnf, metric, value, timestamp)
            if export_to_file:
                with open('{}_{}'.format(vnf, metric), 'a+') as results_file:
                    results_file.write(str(timestamp) + ' ' + str(value) + '\n')
            if export:
                exp_msg = dict(msg)
                exp_msg['mon_period'] = dec_engines.decision_engines[vnf][metric].curr_period
                exp_msg['ewmv'] = dec_engines.decision_engines[vnf][metric].ewmv
                exp_msg['prediction_interval'] = dec_engines.decision_engines[vnf][metric].get_PI()
                export_producer.send(export_topic, exp_msg)


# returns decision messages for all vnf, kpi combinations
def get_all_decision_msgs(dec_engines, config_dict):
    decision_msgs = list()
    for vnf in config_dict['VNFs']:
        metric_decisions = dict()
        for metric in config_dict['VNFs'][vnf]['metrics']:
            if dec_engines.is_active(vnf, metric):
                decision = dec_engines.get_curr_period(vnf, metric)
            else:
                # very long period, so no monitoring occurs
                decision = 1e9
            metric_decisions[metric] = dict()
            metric_decisions[metric]['mon_period'] = decision
        decision_msgs.append({'vnf_name': vnf, 'metrics': metric_decisions})
    return decision_msgs


def get_decision_msgs(dec_engines, config_dict):
    # indicates if any of the metrics is violated
    is_violated_last = False
    are_violated_two_last = False
    decision_msgs = list()
    for vnf in config_dict['VNFs']:
        metric_decisions = dict()
        for metric in config_dict['VNFs'][vnf]['metrics']:
            if dec_engines.is_active(vnf, metric):
                decision = dec_engines.get_decision(vnf, metric)
                if dec_engines.get_tier(vnf, metric) == dec_engines.active_tier:
                    is_violated_last = is_violated_last or dec_engines.is_violated(vnf, metric)
                    if dec_engines.is_violated(vnf, metric):
                        print("violated: {} {}".format(vnf, metric))
                if dec_engines.get_tier(vnf, metric) == dec_engines.active_tier-1:
                    are_violated_two_last = are_violated_two_last or dec_engines.is_violated(vnf, metric)
                are_violated_two_last = are_violated_two_last or is_violated_last
                if decision is not None:
                    metric_decisions[metric] = dict()
                    metric_decisions[metric]['mon_period'] = decision
        if len(metric_decisions) > 0:
            decision_msgs.append({'vnf_name': vnf, 'metrics': metric_decisions})
    # only increase tier if violations in the last tier
    if is_violated_last:
        increased = dec_engines.increase_tier()
        if increased:
            return get_all_decision_msgs(dec_engines, config_dict)
    # only decrease tier if no violations in two last tiers
    elif not are_violated_two_last:
        # check if tier was actually decreased
        decreased = dec_engines.decrease_tier()
        dec_engines.deactivate_above_active()
        if decreased:
            return get_all_decision_msgs(dec_engines, config_dict)
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

osm_consumer = None
osm_producer = None

if osm_export:
    print('Connecting to OSM Kafka cluster at {}...'.format(config['kafka_osm']))

    osm_producer = KafkaProducer(bootstrap_servers=config['kafka_osm'],
                                 value_serializer=lambda m: json.dumps(m, sort_keys=True).encode('ascii'))

    osm_consumer = KafkaConsumer(config['osm_mon_config_topic'], bootstrap_servers=config['kafka_osm'],
                                 value_deserializer=lambda m: json.loads(m.decode('ascii')))

    print('Connected to OSM Kafka cluster')

DEs = DecisionEngines()

# lowest tier is the is the most important
lowest_tier = 0
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
        tier = 0
        if 'tier' in config['VNFs'][vnf]['metrics'][metric]:
            tier = config['VNFs'][vnf]['metrics'][metric]['tier']
        lowest_tier = min(tier, lowest_tier)
        DEs.add_KPI(vnf, metric, mon_period, lower_threshold=lower_threshold, upper_threshold=upper_threshold, tier=tier)

# send initial monitoring periods
init_admin_msgs = get_all_decision_msgs(DEs, config)
for msg in init_admin_msgs:
    print('Sending message: {}'.format(msg))
    admin_producer.send(config['ext_admin_topic'], msg)

export_mon_data = False

while True:
    if osm_export:
        osm_msgs = osm_consumer.poll()
        export_status = get_export_status(osm_msgs)
        if export_status is not None:
            export_mon_data = export_status

    measures = data_consumer.poll()
    feed_measures(DEs, measures, export_mon_data, osm_producer, config['osm_mon_export_topic'])
    admin_msgs = get_decision_msgs(DEs, config)
    for msg in admin_msgs:
        print('Sending message: {}'.format(msg))
        if osm_export and export_mon_data:
            osm_producer.send(config['osm_mon_export_topic'], msg)
        admin_producer.send(config['ext_admin_topic'], msg)
    time.sleep(0.5)
