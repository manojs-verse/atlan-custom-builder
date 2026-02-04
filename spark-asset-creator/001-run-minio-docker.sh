#!/usr/bin/env bash
set -euo pipefail

# Clean up & (re)create network/container
docker rm -f minio >/dev/null 2>&1 || true
docker network create minio-net >/dev/null 2>&1 || true

docker run -d --name minio --network minio-net \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minio -e MINIO_ROOT_PASSWORD=minio123 \
  quay.io/minio/minio server /data --console-address ":9001"

# Wait for MinIO to be ready inside the docker network
until docker run --rm --network minio-net curlimages/curl \
  -sSf http://minio:9000/minio/health/ready >/dev/null; do
  sleep 1
done

MCENV=(-e MC_HOST_local=http://minio:minio123@minio:9000)

# Create bucket and seed the input object
docker run --rm --network minio-net "${MCENV[@]}" minio/mc mb --ignore-existing local/demo-bucket-cust
printf 'cust_id,ccrev,avg_cc_tran_amt,avg_cc_tran_cnt\n1,100,20,3\n' \
| docker run -i --rm --network minio-net "${MCENV[@]}" minio/mc pipe local/demo-bucket-cust/raw/customer_analytics_2025_10.csv

# Verify contents
docker run --rm --network minio-net "${MCENV[@]}" minio/mc ls -r local/demo-bucket-cust

echo "✅ MinIO up: API http://127.0.0.1:9000  Console http://127.0.0.1:9001"
sleep 3