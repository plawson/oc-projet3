PROJECT=localhost:5000/plawson
BTC_TOPOLOGIES=bitcoin-topologies
IMG_NAME=bitcoin-storm-topologies
REGISTRY=192.168.1.101:5000/plawson

all: clean build push

build:
	@cd ..; mvn package
	cp ../target/bitcoin-consumers-1.0-SNAPSHOT.jar .
	docker build -t ${PROJECT}/$(IMG_NAME):latest .

push: build
	docker push ${PROJECT}/$(IMG_NAME):latest

clean:
	@cd ..; mvn clean;
	@rm -f ./bitcoin-consumers-1.0-SNAPSHOT.jar
	$(eval IMAGE_ID=$(shell sh -c "docker images | grep $(IMG_NAME) | grep latest" | awk '{print $$3}'))
	@if [ ! -z $(IMAGE_ID) ]; then \
		docker rmi $(IMAGE_ID) --force; \
	fi;
	$(eval DEPLOY_NAME=$(shell sh -c "kubectl get job | grep $(BTC_TOPOLOGIES)" | awk '{print $$1}'))
	@if [ ! -z $(DEPLOY_NAME) ]; then \
		kubectl delete job  $(DEPLOY_NAME); \
	fi;

deploy: build push
	$(eval ES_CONTAINER_PORT=$(shell sh -c "kubectl get rc es-client -o jsonpath='{.spec.template.spec.containers[0].ports[?(@.name==\"http\")].containerPort}'"))
	$(eval ES_CLUSTER_NAME=$(shell sh -c "kubectl get rc es-client -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name==\"CLUSTER_NAME\")].value}'"))
	$(eval KAFKA_CONTAINER_PORT=$(shell sh -c "kubectl get sts kafka -o jsonpath='{.spec.template.spec.containers[0].ports[0].containerPort}'"))
	kubectl run $(BTC_TOPOLOGIES) --image=${REGISTRY}/$(IMG_NAME):latest \
	--replicas=1 \
	--restart=OnFailure \
	--env="BTC_TX_TOPIC_NAME=btc_tx" \
	--env="BTC_BLK_TOPIC_NAME=btc_blk" \
	--env="BPI_TOPIC_NAME=bpi_eur" \
	--env="KAFKA_HS_SERVICE=kafka-hs" \
	--env="KAFKA_BROKER_PORT=$(KAFKA_CONTAINER_PORT)" \
	--env="ES_CS_SERVICE=elasticsearch" \
	--env="ES_PORT=$(ES_CONTAINER_PORT)" \
	--env="ES_CLUSTER_NAME=$(ES_CLUSTER_NAME)"

.PHONY: all build push clean deploy
