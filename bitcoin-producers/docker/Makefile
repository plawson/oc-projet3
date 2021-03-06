PROJECT=localhost:5000/plawson
BTC_PRODUCERS_TX=bitcoin-producers-tx
BTC_PRODUCERS_BPI=bitcoin-producers-bpi
REGISTRY=192.168.1.101:5000/plawson

all: clean build push

clean: cleanbpi cleantx

deploy: deploybpi deploytx

build:
	docker build -t ${PROJECT}/bitcoin-kafka-producers:latest .

push: build
	docker push ${PROJECT}/bitcoin-kafka-producers:latest

cleantx:
	$(eval IMAGE_ID=$(shell sh -c "docker images | grep bitcoin-kafka-producers | grep latest" | awk '{print $$3}'))
	@if [ ! -z $(IMAGE_ID) ]; then \
		docker rmi $(IMAGE_ID) --force; \
	fi;
	$(eval DEPLOY_NAME=$(shell sh -c "kubectl get deploy | grep $(BTC_PRODUCERS_TX)" | awk '{print $$1}'))
	@if [ ! -z $(DEPLOY_NAME) ]; then \
		kubectl delete deploy  $(DEPLOY_NAME); \
	fi;

cleanbpi:
	$(eval IMAGE_ID=$(shell sh -c "docker images | grep bitcoin-kafka-producers | grep latest" | awk '{print $$3}'))
	@if [ ! -z $(IMAGE_ID) ]; then \
		docker rmi $(IMAGE_ID) --force; \
	fi;
	$(eval DEPLOY_NAME=$(shell sh -c "kubectl get deploy | grep $(BTC_PRODUCERS_BPI)" | awk '{print $$1}'))
	@if [ ! -z $(DEPLOY_NAME) ]; then \
		kubectl delete deploy  $(DEPLOY_NAME); \
	fi;

deploybpi: build push
	$(eval CONTAINER_PORT=$(shell sh -c "kubectl get sts kafka -o jsonpath='{.spec.template.spec.containers[0].ports[0].containerPort}'"))
	kubectl run $(BTC_PRODUCERS_BPI) --image=${REGISTRY}/bitcoin-kafka-producers:latest \
	--env="BTC_TX_TOPIC_NAME=btc_tx" \
	--env="BTC_BLK_TOPIC_NAME=btc_blk" \
	--env="BPI_TOPIC_NAME=bpi_eur" \
	--env="BTC_ZK_STRING=zk-cs.default.svc.cluster.local:2181" \
	--env="KAFKA_HS_SERVICE=kafka-hs" \
	--env="KAFKA_BROKER_PORT=$(CONTAINER_PORT)" \
	--env="PY_PRODUCER_SCRIPT=btc_bpi_kafka_producer.py"

deploytx: build push
	$(eval CONTAINER_PORT=$(shell sh -c "kubectl get sts kafka -o jsonpath='{.spec.template.spec.containers[0].ports[0].containerPort}'"))
	kubectl run $(BTC_PRODUCERS_TX) --image=${REGISTRY}/bitcoin-kafka-producers:latest \
	--env="BTC_TX_TOPIC_NAME=btc_tx" \
	--env="BTC_BLK_TOPIC_NAME=btc_blk" \
	--env="BPI_TOPIC_NAME=bpi_eur" \
	--env="BTC_ZK_STRING=zk-cs.default.svc.cluster.local:2181" \
	--env="KAFKA_HS_SERVICE=kafka-hs" \
	--env="KAFKA_BROKER_PORT=$(CONTAINER_PORT)" \
	--env="PY_PRODUCER_SCRIPT=btc_tx_kafka_producer.py"

.PHONY: all build push clean deploy cleantx cleanbpi deploybpi deploytx
