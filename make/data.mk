.PHONY: network fleet seed-network

network:
	python scripts/build_network.py

fleet: network
	python scripts/select_sim_routes.py --routes 12 --buses 60

seed-network: fleet
	docker compose up --build --force-recreate init-data
