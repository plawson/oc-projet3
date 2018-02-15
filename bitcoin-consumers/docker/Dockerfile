FROM localhost:5000/plawson/kubernetes-storm-base

RUN set -ex \
    && mkdir /btc

ADD run_btc_topologies.sh bitcoin-consumers-1.0-SNAPSHOT.jar /btc/

RUN set -ex \
    && chmod 755 /btc/run_btc_topologies.sh

WORKDIR /opt/apache-storm

CMD ["/btc/run_btc_topologies.sh"]
