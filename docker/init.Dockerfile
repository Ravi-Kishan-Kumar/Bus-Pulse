FROM python:3.11.9-slim-bookworm
WORKDIR /workspace
RUN pip install --no-cache-dir psycopg[binary]==3.2.9 PyYAML==6.0.2
COPY scripts/seed_network.py scripts/seed_network.py
COPY backend/src/ backend/src/
COPY db/migrations/002_auth.sql db/migrations/002_auth.sql
COPY db/migrations/003_bus_profiles.sql db/migrations/003_bus_profiles.sql
COPY db/migrations/004_simulation_regions.sql db/migrations/004_simulation_regions.sql
COPY db/migrations/005_scenarios_and_advisory_buses.sql db/migrations/005_scenarios_and_advisory_buses.sql
COPY db/migrations/006_route_stop_lookup_index.sql db/migrations/006_route_stop_lookup_index.sql
COPY db/migrations/007_unique_advisory_per_alert.sql db/migrations/007_unique_advisory_per_alert.sql
COPY db/migrations/008_crowding_alert_deduplication.sql db/migrations/008_crowding_alert_deduplication.sql
COPY db/migrations/009_local_runtime_retention.sql db/migrations/009_local_runtime_retention.sql
COPY data/blr/network_all.json.gz data/blr/network_all.json.gz
COPY config/sim_fleet.yaml config/sim_fleet.yaml
ENV PYTHONPATH=/workspace/backend/src:/workspace
CMD ["python", "scripts/seed_network.py"]
