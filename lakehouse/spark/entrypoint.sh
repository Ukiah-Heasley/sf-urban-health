#!/usr/bin/env bash
set -euo pipefail

: "${MINIO_ENDPOINT:=http://minio:9000}"
: "${MINIO_ROOT_USER:=minioadmin}"
: "${MINIO_ROOT_PASSWORD:=minioadmin}"
: "${LAKEHOUSE_BUCKET:=lakehouse}"

SPARK_THRIFT_INTERNAL_PORT=10000
WAREHOUSE_PATH="s3a://${LAKEHOUSE_BUCKET}/warehouse"

spark_conf=(
  --master "local[*]"
  --conf "spark.sql.catalog.spark_catalog.warehouse=${WAREHOUSE_PATH}"
  --conf "spark.sql.warehouse.dir=${WAREHOUSE_PATH}"
  --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}"
  --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ROOT_USER}"
  --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_ROOT_PASSWORD}"
)

echo "Bootstrapping lakehouse namespaces in ${WAREHOUSE_PATH}..."
/opt/spark/bin/spark-sql "${spark_conf[@]}" \
  -e "CREATE NAMESPACE IF NOT EXISTS default; CREATE NAMESPACE IF NOT EXISTS sf_urban_health;"

export SPARK_NO_DAEMONIZE=1

exec /opt/spark/sbin/start-thriftserver.sh \
  "${spark_conf[@]}" \
  --hiveconf "hive.server2.thrift.port=${SPARK_THRIFT_INTERNAL_PORT}" \
  --hiveconf "hive.server2.thrift.bind.host=0.0.0.0" \
  --hiveconf "hive.server2.authentication=NOSASL" \
  --hiveconf "hive.server2.enable.doAs=false"
