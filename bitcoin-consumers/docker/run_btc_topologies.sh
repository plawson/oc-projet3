#!/usr/bin/env bash

set -x

function usage {
	echo "$0: [-h] | --btc-tx-topic BTC_TX_TOPIC_NAME --btc-blk-topic BTC_BLK_TOPIC_NAME --bpi-topic BPI_TOPIC_NAME --kafka-hs-service KAFKA_HS_SERVICE --kafka-broker-port KAFKA_BROKER_PORT --es-cs-service ES_CS_SERVICE -es-port ES_PORT -es-cluster ES_CLUSTER_NAME"
	exit 1
}


POSITIONAL=()
while [[ $# -gt 0 ]]
do
	key="$1"
	case $key in
		--btc-tx-topic)
		BTC_TX_TOPIC_NAME="$2"
		shift # past argument
		shift # past value
		;;
		--btc-blk-topic)
		BTC_BLK_TOPIC_NAME="$2"
		shift # past argument
		shift # past value
		;;
		--bpi-topic)
		BPI_TOPIC_NAME="$2"
		shift # past argument
		shift # past value
		;;
		--kafka-hs-service)
		KAFKA_HS_SERVICE="$2"
		shift # past argument
		shift # past value
		;;
		--kafka-broker-port)
		KAFKA_BROKER_PORT="$2"
		shift # past argument
		shift # past value
		;;
		--es-cs-service)
		ES_CS_SERVICE="$2"
		shift # past argument
		shift # past value
		;;
		--es-port)
		ES_PORT="$2"
		shift # past argument
		shift # past value
		;;
		--es-cluster)
		ES_CLUSTER_NAME="$2"
		shift # past argument
		shift # past value
		;;
		-h)
		usage
		;;
		*) # unknown option
		POSITIONAL+=("$1") # save it in an array for later
		shift # past argument
		;;
	esac
done

#set -- "${POSITIONAL[@]}" # restore positional parameters

# remove spaces
BTC_TX_TOPIC_NAME="${BTC_TX_TOPIC_NAME// }"
BTC_BLK_TOPIC_NAME="${BTC_BLK_TOPIC_NAME// }"
BPI_TOPIC_NAME="${BPI_TOPIC_NAME// }"
BTC_ZK_STRING="${BTC_ZK_STRING// }"
KAFKA_HS_SERVICE="${KAFKA_HS_SERVICE// }"
KAFKA_BROKER_PORT="${KAFKA_BROKER_PORT// }"
ES_CS_SERVICE="${ES_CS_SERVICE// }"
ES_PORT="${ES_PORT// }"
ES_CLUSTER_NAME="${ES_CLUSTER_NAME// }"


# Check input parameters
if [[  -z "$BTC_TX_TOPIC_NAME" || -z "$BTC_BLK_TOPIC_NAME" || -z "$BPI_TOPIC_NAME" ]]; then
	echo "ERROR: --btc-tx-topic, --btc-blk-topic and --bpi-topic are mandatory for btx producer"
	usage
fi

if [[ -z "$KAFKA_HS_SERVICE" ]]; then
    echo "ERROR: Kafka headless service name is mandatory"
    usage
fi

if [[ -z "$KAFKA_BROKER_PORT" ]]; then
    echo "ERROR: Kafka broker port is mandatory"
    usage
fi

if [[ -z "$ES_CS_SERVICE" ]]; then
    echo "ERROR: Elasticsearch service name is mandatory"
    usage
fi

if [[ -z "$ES_PORT" ]]; then
    echo "ERROR: Elasticsearch port is mandatory"
    usage
fi

if [[ -z "$ES_CLUSTER_NAME" ]]; then
    echo "ERROR: Elasticsearch cluster name is mandatory"
    usage
fi


/configure.sh ${ZK_CS_SERVICE_HOST:-$1} ${NIMBUS_SERVICE_HOST:-$2}

cd /opt/apache-storm
bin/storm jar /btc/bitcoin-consumers-1.0-SNAPSHOT.jar bitcoin.BitcoinMonitoring remote
