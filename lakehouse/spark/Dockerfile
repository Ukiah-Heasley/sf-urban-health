# Local Spark Thrift Server with Iceberg and S3A (MinIO) / AWS Glue support.
ARG SPARK_VERSION=3.5.4
FROM apache/spark:${SPARK_VERSION}

USER root

ARG ICEBERG_VERSION=1.6.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG SPARK_MAJOR_MINOR=3.5

RUN set -eux; \
    curl -fsSL -o "/opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_MINOR}_2.12-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_MINOR}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_MINOR}_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fsSL -o "/opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fsSL -o "/opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSL -o "/opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY lakehouse_catalog_config.py /opt/spark/lakehouse_catalog_config.py
COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

USER spark

EXPOSE 10000 4040

ENTRYPOINT ["/opt/spark/entrypoint.sh"]
