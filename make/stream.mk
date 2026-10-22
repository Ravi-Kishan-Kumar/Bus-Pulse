.PHONY: topics train-ml

topics:
	docker compose up --force-recreate init-kafka

train-ml:
	docker compose --profile tools run --rm ml-train
