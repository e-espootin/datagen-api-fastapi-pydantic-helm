# include .env
# export


.PHONY: install run test clean

TOPIC ?=taxitrips
KAFKA_RELEASE ?=kafka-plain
MESSAGES_BATCH_SIZE ?=5

install:
	uv pip install -r requirements.txt
	
run:
	cd src && uv run python main.py

dev:
	. .venv/bin/activate && cd src && python main.py

docker-build:
	docker build -t e4espootin/kafka_api_consumer .
	docker push e4espootin/kafka_api_consumer:latest

docker-dev:
	docker build -t e4espootin/kafka_api_consumer .
	docker run -it -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 -e topic=taxitrips e4espootin/kafka_api_consumer 


clean:
	rm -rf .venv
	find . -type d -name "__pycache__" -exec rm -r {} +