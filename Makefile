.PHONY: install etl api webhook anomalies test

install:
	pip install -r requirements.txt

etl:
	python etl/generate_sample_data.py
	python etl/transform.py
	python etl/load_mart.py

api:
	uvicorn api.main:app --reload

webhook:
	uvicorn automation.webhook_receiver:app --port 8010 --reload

anomalies:
	python ai/anomaly.py

test:
	pytest -q
