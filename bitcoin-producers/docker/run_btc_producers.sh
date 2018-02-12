#!/usr/bin/env bash

set -x

function usage {
	echo "$0: [-h] | --btc-tx-topic BTC_TX_TOPIC_NAME --btc-blk-topic BTC_BLK_TOPIC_NAME --bpi-topic BPI_TOPIC_NAME --zookeeper BTC_ZK_STRING --brokers BTC_BROKERS"
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
		--zookeeper)
		BTC_ZK_STRING="$2"
		shift # past argument
		shift # past value
		;;
		--brokers)
		BTC_BROKERS="$2"
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
BTC_BROKERS="${BTC_BROKERS// }"

# Check input parameters
if [[  -z "$BTC_TX_TOPIC_NAME" || -z "$BTC_BLK_TOPIC_NAME" || -z "$BPI_TOPIC_NAME" ]]; then
	echo "ERROR: --btc-tx-topic, --btc-blk-topic and --bpi-topic are mandatory for btx producer"
	usage
fi

if [[ -z "$BTC_ZK_STRING" ]]; then
	echo "ERROR: Zookeeper connect string is mandatory"
	usage
fi

if [[ -z "$BTC_BROKERS" ]]; then
    echo "ERROR: Brokers connect string is mandatory"
    usage
fi


cd /opt/kafka
TOPIC_CMD="bin/kafka-topics.sh --zookeeper $BTC_ZK_STRING"

# Create missing topics
for topic in $BTC_BLK_TOPIC_NAME $BPI_TOPIC_NAME; do
    RESULT=$($TOPIC_CMD --list | egrep "^"$topic"$" 2>&1 1>/dev/null; echo $?)
    if [[ $RESULT -ne 0 ]]; then
        $TOPIC_CMD --create --topic $topic --partitions 1 --replication-factor 2
        if [[ $? -ne 0 ]]; then
            echo "ERROR: Cannot create topic $topic"
            exit 1
        fi
    fi
done

RESULT=$($TOPIC_CMD --list | egrep "^"$BTC_TX_TOPIC_NAME"$" 2>&1 1>/dev/null; echo $?)
if [[ $RESULT -ne 0 ]]; then
    $TOPIC_CMD --create --topic $BTC_TX_TOPIC_NAME --partitions 10 --replication-factor 2
    if [[ $? -ne 0 ]]; then
        echo "ERROR: Cannot create topic $BTC_TX_TOPIC_NAME"
        exit 1
    fi
fi

# Starting producers
BTC_TOPICS="--btc-tx-topic $BTC_TX_TOPIC_NAME --btc-blk-topic $BTC_BLK_TOPIC_NAME --bpi-topic $BPI_TOPIC_NAME"
/btc/btc_kafka_producer.py --bokers $BTC_BROKERS $BTC_TOPICS
