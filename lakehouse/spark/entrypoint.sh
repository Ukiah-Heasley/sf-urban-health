#!/usr/bin/env bash
set -euo pipefail

: "${LAKEHOUSE_CATALOG:=hadoop}"

mapfile -t spark_conf < <(python3 /opt/spark/lakehouse_catalog_config.py --shell-args)
bootstrap_sql="$(python3 /opt/spark/lakehouse_catalog_config.py --bootstrap-sql)"

warehouse_path="$(python3 /opt/spark/lakehouse_catalog_config.py --warehouse-uri)"

echo "Bootstrapping lakehouse namespaces (${LAKEHOUSE_CATALOG} catalog) in ${warehouse_path}..."
/opt/spark/bin/spark-sql "${spark_conf[@]}" -e "${bootstrap_sql}"

export SPARK_NO_DAEMONIZE=1

exec /opt/spark/sbin/start-thriftserver.sh \
  "${spark_conf[@]}" \
  --hiveconf "hive.server2.thrift.port=10000" \
  --hiveconf "hive.server2.thrift.bind.host=0.0.0.0" \
  --hiveconf "hive.server2.authentication=NOSASL" \
  --hiveconf "hive.server2.enable.doAs=false"
