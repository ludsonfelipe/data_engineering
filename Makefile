# Minio
minio_console:
	docker exec -it minio_polars bash
minio_raw_layer:
	docker exec -it minio_polars bash -c "mc rm --force --dangerous data/raw; mc mb data/raw"
minio_lakehouse_layer:
	docker exec -it minio_polars bash -c "mc rm --force --dangerous data/lakehouse; mc mb data/lakehouse"
minio_layers: minio_raw_layer minio_lakehouse_layer