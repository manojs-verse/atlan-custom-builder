#!/usr/bin/env bash
set -euo pipefail

#ATLAN_API_KEY=$1

export ATLAN_API_KEY_FILE=$(pwd)/atlan_token.txt

spark-submit \
 --packages io.openlineage:openlineage-spark_2.12:1.38.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
 --conf "spark.openlineage.url=https://home.atlan.com/api/openlineage" \
 --conf "spark.openlineage.apiKey=$(cat $ATLAN_API_KEY_FILE)" \
 --conf spark.hadoop.fs.s3a.access.key=minio \
 --conf spark.hadoop.fs.s3a.secret.key=minio123 \
 --conf spark.hadoop.fs.s3a.path.style.access=true \
 --conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.security=ALL-UNNAMED" \
 --conf "spark.executor.extraJavaOptions=--add-opens=java.base/java.security=ALL-UNNAMED" \
 spark_ol_minio.py
