PROJECT=localhost:5000/plawson
BTC_PRODUCERS=bitcoin-producers

all: build

build:
	docker build -t ${PROJECT}/bitcoin-kafka-producers:latest .

push: build
	docker push ${PROJECT}/bitcoin-kafka-producers:latest

clean:
	$(eval IMAGE_ID=$(shell sh -c "docker images | grep bitcoin-kafka-producers | grep latest" | awk '{print $$3}'))
	docker rmi $(IMAGE_ID) --force
	$(eval DEPLOY_NAME=$(shell sh -c "kubectl get deploy | grep $(BTC_PRODUCERS)" | awk '{print $$1}'))
	@if [ ! -z $(DEPLOY_NAME) ]; then \
		kubectl delete deploy  $(DEPLOY_NAME); \
	fi;

deploy:
	kubectl run $(BTC_PRODUCERS) --image=${PROJECT}/bitcoin-kafka-producers:latest \
	--env="BTC_TX_TOPIC_NAME=btc_tx" \
	--env="BTC_BLK_TOPIC_NAME=btc_blk" \
	--env="BPI_TOPIC_NAME=bpi_eur" \
	--env="BTC_ZK_STRING=zk-cs.default.svc.cluster.local:2181" \
	--env="BTC_BROKERS=kafka-1.kafka-hs.default.svc.cluster.local:9093,kafka-2.kafka-hs.default.svc.cluster.local:9093,kafka-0.kafka-hs.default.svc.cluster.local:9093"

.PHONY: all build push clean
