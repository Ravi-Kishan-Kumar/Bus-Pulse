# Data sources and provenance

## BMTC route network

- Feed: MobilityDatabase `mdb-2595`, Bangalore Metropolitan Transport Corporation (BMTC), GTFS Schedule, Community Feed.
- Catalog: <https://mobilitydatabase.org/feeds/gtfs/mdb-2595>
- Map preview: <https://mobilitydatabase.org/feeds/gtfs/mdb-2595/map>
- Producer: Vonter's `bmtc-gtfs` repository, <https://github.com/Vonter/bmtc-gtfs>.
- Producer states the dataset is under ODbL-1.0; some individual contents are BMTC copyright. Keep attribution and share-alike notices with redistributed adapted data. Confirm terms with course staff before publishing the derived network.
- The MobilityDatabase record identifies 4,434 bus routes, route shapes, and a current service date range. It shows a clean GTFS validator result. This checks file/schema consistency, not the accuracy or operational status of every route.
- The downloaded archive is 44,392,885 bytes (44.39 MB); `feed_info.txt` reports version `20260907`, with service dates `2026-09-07` to `2027-09-07`. The downloaded archive's SHA-256 is recorded in `data/blr/PROVENANCE.json`.
- Local geometry screening of this snapshot produced **3,633 usable routes**, **6,032 usable route directions**, and **8,771 stops covered**. The output exceeds the project quality floors (3,000 usable routes and 8,000 covered stops). The exact measurements are in `reports/network_quality.json`.
- The demo fleet selector chose 12 routes with five buses each for the initial 60-bus configuration. Each route count can be edited independently; administrator controls will apply a bounded live change.
- The feed maintainer warns that some schedules and stop times are inaccurate and that missing routes may reflect tracking-feed availability. This project uses stops, stop sequence, and route geometry only. It does not use schedules as live arrival information.
- Vehicle locations, passenger counts, occupancy, demand, and incidents are synthetic. GTFS contains no live occupancy feed.

## Build-time checks

`scripts/build_network.py` records the GTFS `feed_version`, service dates, producer attribution, and SHA-256 in `data/blr/PROVENANCE.json`. It screens route directions using stop-count, Bengaluru bounding box, stop-to-shape distance, shape/stop-distance ratio, ordering, and maximum-length checks. Human-review `reports/network_preview.html` before treating routes as usable.

Never invent route geometry. A route that fails screening remains in the network extract with `usable=false` and a reason.
