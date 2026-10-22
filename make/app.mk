.PHONY: up down logs report

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

report:
	python -m json.tool reports/network_quality.json
