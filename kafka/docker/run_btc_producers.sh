#!/usr/bin/env bash

function usage {
	echo "$0: [-h] | (--producer btx --btc-tx-topic BTC_TX_TOPIC_NAME --btc-blk-topic BTC_BLK_TOPIC_NAME|--producer bpi --bpi-topic BPI_TOPIC_NAME) --zookeeper BTC_ZK_STRING"
	exit 1
}

FOUND_PRODUCER=false
POSITIONAL=()
while [[ $# -gt 0 ]]
do
	key="$1"
	case $key in
		--producer)
		if [[ $FOUND_PRODUCER ]];then
			echo "ERROR: Only one producer allowed"
			usage
		fi
		BTC_PRODUCER="$2"
		FOUND_PRODUCER=true
		shift # past argument
		shift # past value
		;;
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
BTC_PRODUCER="${BTC_PRODUCER// }"
BTC_TX_TOPIC_NAME="${BTC_TX_TOPIC_NAME// }"
BTC_BLK_TOPIC_NAME="${BTC_BLK_TOPIC_NAME// }"
BPI_TOPIC_NAME="${BPI_TOPIC_NAME// }"
BTC_ZK_STRING="${BTC_ZK_STRING// }"

# Check input parameters
if [[ -z "$BTC_PRODUCER" ]]; then
	echo "ERROR: Producer is mandatory"
	usage
elif [[ "$BTC_PRODUCER" == "btx" && ( -z "$BTC_TX_TOPIC_NAME" || -z "$BTC_BLK_TOPIC_NAME") ]]; then
	echo "ERROR: Both --btc-tx-topic and --btc-blk-topic are mandatory for btx producer"
	usage
elif [[ "$BTC_PRODUCER" == "bpi" &&  -z "$BPI_TOPIC_NAME" ]]; then
	echo "ERROR: --bpi-topic is mandatory for bpi producer"
	usage
fi


if [[ -z "$BTC_ZK_STRING" ]]; then
	echo "ERROR: Zookeeper connect string is mandatory"
	usage
fi



cd /opt/kafka
TOPIC_CMD="bin/kafka-topics.sh --zookeeper $BTC_ZK_STRING"

# Create missing topics
if [[ "$BTC_PRODUCER" == "btx" ]]; then

	BTC_TOPICS="--btc-tx-topic $BTC_TX_TOPIC_NAME --btc-blk-topic $BTC_BLK_TOPIC_NAME"

	for topic in "$BTC_TX_TOPIC_NAME $BTC_BLK_TOPIC_NAME"; do
		RESULT=$($TOPIC_CMD --list | egrep "^"$topic"$" 2>&1 1>/dev/null; echo $?)
		if [[ $RESULT -ne 0 ]]; then
			$TOPIC_CMD --create --topic $topic
			if [[ $? -ne 0 ]]; then
				echo "ERROR: Cannot create topic $topic"
				exit 1
			fi
		fi
	done
elif [[ "$BTC_PRODUCER" == "bpi" ]]; then

	BTC_TOPICS="--bpi-topic $BPI_TOPIC_NAME"

	RESULT=$($TOPIC_CMD --list | egrep "^"$BPI_TOPIC_NAME"$" 2>&1 1>/dev/null; echo $?)
	if [[ $RESULT -ne 0 ]]; then
		$TOPIC_CMD --create --topic $BPI_TOPIC_NAME
		if [[ $? -ne 0 ]]; then
			echo "ERROR: Cannot create topic $BPI_TOPIC_NAME"
			exit 1
		fi
	fi
fi

/btc/btc_kafka_producer.py --zookeeper $BTC_ZK_STRING --producer $BTC_PRODUCER $BTC_TOPICS
