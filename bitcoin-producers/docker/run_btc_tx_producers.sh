#!/usr/bin/env bash

set -x

function usage {
	echo "$0: [-h] | --btc-tx-topic BTC_TX_TOPIC_NAME --btc-blk-topic BTC_BLK_TOPIC_NAME --bpi-topic BPI_TOPIC_NAME --zookeeper BTC_ZK_STRING --kafka-hs-service KAFKA_HS_SERVICE --kafka-broker-port KAFKA_BROKER_PORT"
	exit 1
}

POSITIONAL=()
while [[ $# -gt 0 ]]
do
	key="$1"
	case ${key} in
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
		--zookeeper)
		BTC_ZK_STRING="$2"
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
PY_PRODUCER_SCRIPT="${PY_PRODUCER_SCRIPT// }"

# Check python script
if [[ -z "$PY_PRODUCER_SCRIPT" || ! -x "/btc/$PY_PRODUCER_SCRIPT" ]]; then
	echo "ERROR: env variable PY_PRODUCER_SCRIPT is not properly set"
fi

# Check input parameters
if [[  -z "$BTC_TX_TOPIC_NAME" || -z "$BTC_BLK_TOPIC_NAME" || -z "$BPI_TOPIC_NAME" ]]; then
	echo "ERROR: --btc-tx-topic, --btc-blk-topic and --bpi-topic are mandatory for btx producer"
	usage
fi

if [[ -z "$BTC_ZK_STRING" ]]; then
	echo "ERROR: Zookeeper connect string is mandatory"
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


cd /opt/kafka
TOPIC_CMD="bin/kafka-topics.sh --zookeeper $BTC_ZK_STRING"

# Create missing topics
for topic in ${BTC_BLK_TOPIC_NAME} ${BPI_TOPIC_NAME}; do
    RESULT=$(${TOPIC_CMD} --list | egrep "^"${topic}"$" 2>&1 1>/dev/null; echo $?)
    if [[ ${RESULT} -ne 0 ]]; then
        ${TOPIC_CMD} --create --topic ${topic} --partitions 1 --replication-factor 3
        if [[ $? -ne 0 ]]; then
            echo "ERROR: Cannot create topic $topic"
            exit 1
        fi
    fi
done

RESULT=$(${TOPIC_CMD} --list | egrep "^"${BTC_TX_TOPIC_NAME}"$" 2>&1 1>/dev/null; echo $?)
if [[ ${RESULT} -ne 0 ]]; then
    ${TOPIC_CMD} --create --topic ${BTC_TX_TOPIC_NAME} --partitions 10 --replication-factor 3
    if [[ $? -ne 0 ]]; then
        echo "ERROR: Cannot create topic $BTC_TX_TOPIC_NAME"
        exit 1
    fi
fi

# Starting producers
BTC_TOPICS="--btc-tx-topic $BTC_TX_TOPIC_NAME --btc-blk-topic $BTC_BLK_TOPIC_NAME --bpi-topic $BPI_TOPIC_NAME"
/btc/${PY_PRODUCER_SCRIPT} --kafka-hs-service ${KAFKA_HS_SERVICE} --kafka-broker-port ${KAFKA_BROKER_PORT} ${BTC_TOPICS}
