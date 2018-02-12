#!/usr/bin/env python3

from kafka import KafkaProducer
from time import time
from datetime import datetime, timezone
import argparse
import logging
import logging.config
import json
import websocket
import requests


class Producer:
    def __init__(self, brokers, topics):
        self.logger = logging.getLogger('producer')
        self.logger.debug('brokers: {}'.format(brokers))
        self.logger.debug('Creating KafkaProducer')
        self.producer = KafkaProducer(bootstrap_servers=brokers)
        if None is topics:
            raise Exception("topics is mandatory")
        self.topics = topics
        for key, value in topics.items():
            self.logger.debug('{}: {}'.format(key, value))
        self._bpi_retry = 0

    @property
    def bpi_retry(self):
        return self._bpi_retry

    @bpi_retry.setter
    def bpi_retry(self, val):
        self._bpi_retry = val

    def process_inputs(self):
        ws = self.open_websocket_to_blockchain()

        last_ping_time = time()
        last_bpi_time = time()

        self.logger.debug('Extracting BPI data 1...')
        bpi_data = self.extract_eur_index(self.get_bpi())
        self.logger.info('Sending BPI record 1...')
        self.producer.send(self.topics['bpi-topic'], json.dumps(bpi_data).encode())

        while True:
            data = json.loads(ws.recv())  # Receive event
            # We ping the server every 10s to show we are alive
            if time() - last_ping_time >= 10:
                ws.send(json.dumps({"op": "ping"}))
                last_ping_time = time()

            # Get BPI every minute
            if time() - last_bpi_time >= 60:
                self.logger.debug('Extracting BPI data...')
                bpi_data = self.extract_eur_index(self.get_bpi())
                self.logger.info('Sending BPI record...')
                self.producer.send(self.topics['bpi-topic'], json.dumps(bpi_data).encode())
                last_bpi_time = time()

            # Response to "ping" events
            if data["op"] == "pong":
                pass

            # New unconfirmed transactions
            elif data["op"] == "utx":
                transaction_timestamp = data["x"]["time"]
                transaction_hash = data['x']['hash']  # this uniquely identifies the transaction
                transaction_total_amount = 0

                for recipient in data["x"]["out"]:
                    # Every transaction may in fact have multiple recipients
                    # Note that the total amount is in hundredth of microbitcoin; you need to
                    # divide by 10**8 to obtain the value in bitcoins.
                    transaction_total_amount += recipient["value"] / 100000000.

                tx_data = {'btc_timestamp': transaction_timestamp, 'tx_id': transaction_hash,
                           'tx_btc_amount': transaction_total_amount,
                           'tx_eur_amount': transaction_total_amount * bpi_data['rate_float']}
                self.logger.info('Sending BTC Tx record...')
                self.producer.send(self.topics['btc-tx-topic'], json.dumps(tx_data).encode())

            # New block
            elif data["op"] == "block":
                block_hash = data['x']['hash']
                block_timestamp = data["x"]["time"]
                block_found_by = data["x"]["foundBy"]["description"]
                block_reward = 12.5  # blocks mined in 2016 have an associated reward of 12.5 BTC

                blk_data = {'btc_timestamp': block_timestamp, 'blk_id': block_hash, 'blk_owner': block_found_by,
                            'blk_btc_reward': block_reward, 'blk_eur_reward': block_reward * bpi_data['rate_float']}
                self.logger.info('Sending BTC Blk record...')
                self.producer.send(self.topics['bpi-topic'], json.dumps(blk_data).encode())

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

    def open_websocket_to_blockchain(self):
        self.logger.debug('Opening websocket...')
        ws = websocket.WebSocket()  # Open a websocket
        self.logger.debug('Connecting websocket...')
        ws.connect("wss://ws.blockchain.info/inv")
        self.logger.debug('Registering to unconfirmed transaction events...')
        ws.send(json.dumps({"op": "unconfirmed_sub"}))  # Register to unconfirmed transaction events
        self.logger.debug('Registering to block creation events...')
        ws.send(json.dumps({"op": "blocks_sub"}))  # Register to block creation events
        return ws

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
    logger.info('Started btc_kafka_producers.py')

    logger.debug('Initializing argument parser...')
    parser = argparse.ArgumentParser()
    parser.add_argument('--btc-tx-topic', help="Topic name for bitcoin transaction", required=True)
    parser.add_argument('--btc-blk-topic', help="Topic name for new block", required=True)
    parser.add_argument('--bpi-topic', help="Topic name for bitcoin price index", required=True)
    parser.add_argument('--bokers', help="Bootstrap servers", required=True)

    logger.debug('Parsing arguments...')
    args = parser.parse_args()

    for arg in vars(args):
        logger.debug('key: {}, value: {}'.format(arg, getattr(args, arg)))

    logger.debug('Instantiating Producer')
    producer = Producer(vars(args)['bokers'], {'btc-tx-topic': vars(args)['btc_tx_topic'],
                                               'btc-blk-topic': vars(args)['btc_blk_topic'],
                                               'bpi-topic': vars(args)['bpi_topic']})
    logger.info('Starting producer...')
    producer.process_inputs()
