include .env
export


.PHONY: install run test clean

DSNAME ?=taxitrips
API_LOCAL_URL ?=http://localhost:8080
KAFKA_BOOTSTRAP_SERVERS ?=localhost:9092
ALL_DATASETS ?='{"taxitrips":"/taxitrips/?num_samples=1"}'
SLEEP_TIME_SEC ?= 3

install:
	uv pip install -r requirements.txt
	
run:
	python src/main.py 
runuv:
	cd src && uv run python main.py

dev:
	python src/main.py -d
devuv:
	cd src && uv run python main.py -d
	
docker-build:
	docker build -t e4espootin/kafka_api_producer .
	docker push e4espootin/kafka_api_producer:latest

docker-dev:
	docker build -t e4espootin/kafka_api_producer .
	docker run -it e4espootin/kafka_api_producer 

