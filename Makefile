up:
	docker compose up -d zookeeper kafka minio

ingest-osm:
	docker compose run --rm collector-osm

ingest-trends:
	docker compose run --rm collector-google-trends

ingest-chotot:
	docker compose run --rm crawler-chotot
	docker compose run --rm consumer-chotot-bronze

validate-bronze:
	docker compose run --rm validate-bronze

phase-1: up ingest-osm ingest-trends ingest-chotot validate-bronze