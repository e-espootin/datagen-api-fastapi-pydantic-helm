export PYTHONPATH := $(PWD):$(PYTHONPATH)


.PHONY: run, docker-run

install:
	uv pip install -r requirements.txt
	
run:
	echo $(PYTHONPATH)
	uv run uvicorn app.main:app --host 0.0.0.0 --port 8080

dev:
	echo $(PYTHONPATH)
	uv run fastapi dev app/main.py --root-path /api/v1 --reload --host 0.0.0.0 --port 8080

docker-run:
	docker rm -f xx || true
	docker run --rm -p 8080:8080 --name xx e4espootin/datagen_api

docker-build:
	docker build --platform linux/amd64 -t e4espootin/datagen_api .
	docker push e4espootin/datagen_api
	