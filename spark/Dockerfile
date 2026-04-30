# spark/Dockerfile
# -------------------------------------------------------
# Spark + Python CDC Processor container
# Based on official Apache Spark 3.5.1 image (Python included)
# -------------------------------------------------------
FROM apache/spark:3.5.1

# Switch to root to install packages
USER root

WORKDIR /app

# Install pip dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install dos2unix to fix Windows CRLF line endings in shell scripts
RUN apt-get update && apt-get install -y --no-install-recommends dos2unix \
    && rm -rf /var/lib/apt/lists/*

# Copy application source
COPY app/cdc_processor.py .
COPY app/alert.py .
COPY app/test_dq_alert.py .
COPY entrypoint.sh .
# Fix CRLF → LF and make executable (critical when editing on Windows)
RUN dos2unix /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Pre-fetch required JARs at build time so no internet needed at runtime.
# Versions are pinned to match Spark 3.5.1 / Scala 2.12.
ENV SPARK_AVRO_VERSION=3.5.1
ENV SPARK_SCALA_VERSION=2.12
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
# Deequ 2.0.7 — compatible with Spark 3.5.x
ENV DEEQU_VERSION=2.0.7-spark-3.5

RUN mkdir -p /opt/spark/extra-jars && \
    # spark-avro (must match Spark version)
    curl -fL -o /opt/spark/extra-jars/spark-avro.jar \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_${SPARK_SCALA_VERSION}/${SPARK_AVRO_VERSION}/spark-avro_${SPARK_SCALA_VERSION}-${SPARK_AVRO_VERSION}.jar" && \
    # hadoop-aws (S3A filesystem)
    curl -fL -o /opt/spark/extra-jars/hadoop-aws.jar \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    # aws-java-sdk-bundle (required by hadoop-aws)
    curl -fL -o /opt/spark/extra-jars/aws-java-sdk-bundle.jar \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" && \
    # deequ (Amazon Deequ data quality library)
    curl -fL -o /opt/spark/extra-jars/deequ.jar \
      "https://repo1.maven.org/maven2/com/amazon/deequ/deequ/${DEEQU_VERSION}/deequ-${DEEQU_VERSION}.jar"

# Tell spark-submit to include the extra JARs by default
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/extra-jars/*

ENTRYPOINT ["/app/entrypoint.sh"]
