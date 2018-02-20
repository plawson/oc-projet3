FROM localhost:5000/plawson/kubernetes-kafka

RUN set -ex \
    && apt-get update \
    && apt-get install -y python3-pip \
    && pip3 install websocket-client \
    && pip3 install kafka-python \
    && pip3 install requests \
    && apt-get install -y vim

RUN set -ex \
    && mkdir /btc

ADD run_btc_producers.sh btc_kafka_producer.py btc_logging.conf /btc/

RUN set -ex \
    && chmod 755 /btc/run_btc_producers.sh \
    && chmod 755 /btc/btc_kafka_producer.py \
    && chmod 644 /btc/btc_logging.conf

CMD ["/btc/run_btc_producers.sh"]
