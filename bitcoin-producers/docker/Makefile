PROJECT=localhost:5000/plawson

all: build

build:
	docker build -t ${PROJECT}/bitcoin-kafka-producers:latest .

push: build
	docker push ${PROJECT}/bitcoin-kafka-producers:latest

.PHONY: all build push
