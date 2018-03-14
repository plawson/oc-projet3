#!/usr/bin/env python3

from kafka import KafkaProducer
from time import time
from datetime import datetime, timezone
import argparse
import logging
import logging.config
import json
import requests
import socket


class Producer:
    def __init__(self, service_name, port, topic):
        self.logger = logging.getLogger('producer')
        self.logger.debug('service: {}'.format(service_name))
        self.logger.debug('port: {}'.format(port))
        self.logger.info('Retrieving brokers list...')
        brokers = ""
        (hostname, aliaslist, ipaddrlist) = socket.gethostbyname_ex(service_name)
        for ipaddr in ipaddrlist:
            (hostname, aliaslist, hostip) = socket.gethostbyaddr(ipaddr)
            brokers += '{}:{},'.format(hostname, port)
        brokers = brokers[:-1]
        self.logger.info('bootstrap_servers: {}'.format(brokers))
        self.logger.debug('Creating KafkaProducer')
        self.producer = KafkaProducer(bootstrap_servers=brokers)
        if None is topic:
            raise Exception("topic is mandatory")
        self.topic = topic
        self.logger.debug('Topic name: {}'.format(self.topic))
        self._bpi_retry = 0

    @property
    def bpi_retry(self):
        return self._bpi_retry

    @bpi_retry.setter
    def bpi_retry(self, val):
        self._bpi_retry = val

    def process_inputs(self):
        self.logger.info('BTC BPI Producer started')

        last_bpi_time = time()

        self.logger.debug('Extracting BPI data 1...')
        bpi_data = self.extract_eur_index(self.get_bpi())
        self.logger.debug('Sending BPI record 1...')
        self.producer.send(self.topic, json.dumps(bpi_data).encode())

        while True:
            # Get BPI every minute
            if time() - last_bpi_time >= 60:
                self.logger.debug('Extracting BPI data...')
                bpi_data = self.extract_eur_index(self.get_bpi())
                self.logger.debug('Sending BPI record...')
                self.producer.send(self.topic, json.dumps(bpi_data).encode())
                last_bpi_time = time()

    def get_bpi(self):
        resp = requests.get('https://api.coindesk.com/v1/bpi/currentprice.json')
        if resp.status_code != 200:
            self.logger.error('Error accessing BPI REST endpoint - HTTP code: {}'.format(resp.status_code))
            self.bpi_retry += 1
            if not self.bpi_retry < 3:
                self.logger.error('Backing off...')
                raise Exception('GET currentprice.json {}'.format(resp.status_code))
        self.bpi_retry = 0
        return resp.json()

    def extract_eur_index(self, bpi):
        bpi_data = {}
        iso_date = bpi['time']['updatedISO']
        unix_date = datetime(int(iso_date[0:4]), int(iso_date[5:7]), int(iso_date[8:10]), int(iso_date[11:13]),
                             int(iso_date[14:16]), int(iso_date[17:19]), tzinfo=timezone.utc).timestamp()
        self.logger.debug('Building BPI data...')
        bpi_data['btc_timestamp'] = unix_date
        bpi_data['rate_float'] = bpi['bpi']['EUR']['rate_float']
        bpi_data['currency'] = 'EUR'
        return bpi_data


if __name__ == "__main__":
    logging.config.fileConfig('/btc/btc_logging.conf')
    logger = logging.getLogger('root')
    logger.info('Started btc_bpi_kafka_producers.py')

    logger.debug('Initializing argument parser...')
    parser = argparse.ArgumentParser()
    parser.add_argument('--btc-tx-topic', help="Topic name for bitcoin transaction")
    parser.add_argument('--btc-blk-topic', help="Topic name for new block")
    parser.add_argument('--bpi-topic', help="Topic name for bitcoin price index", required=True)
    parser.add_argument('--kafka-hs-service', help="Kafka kubernetes headless service name", required=True)
    parser.add_argument('--kafka-broker-port', help="Kafka broker port", required=True)

    logger.debug('Parsing arguments...')
    args = parser.parse_args()

    for arg in vars(args):
        logger.debug('key: {}, value: {}'.format(arg, getattr(args, arg)))

    logger.debug('Instantiating Producer')
    producer = Producer(vars(args)['kafka_hs_service'], vars(args)['kafka_broker_port'], vars(args)['bpi_topic'])
    logger.info('Starting producer...')
    producer.process_inputs()
