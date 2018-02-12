FROM localhost:5000/plawson/kubernetes-kafka

RUN set -ex \
    && apt-get update \
    && apt-get install -y python3-pip \
    && pip3 install websocket-client \
    && pip3 install kafka-python

RUN set -ex \
    && mkdir /btc

ADD run_btc_producers.sh btc_kafka_producer.py /btc/

CMD ["/btc/run_btc_producers.sh"]
