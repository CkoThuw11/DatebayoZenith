FROM apache/spark:3.5.1

USER root
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    dos2unix \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY spark-app/ /app/
COPY entrypoint.sh /app/

RUN dos2unix /app/entrypoint.sh && chmod +x /app/entrypoint.sh

ENV SPARK_VERSION=3.5.1
ENV SCALA_VERSION=2.12
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV DEEQU_VERSION=2.0.7-spark-3.5

ENV MAVEN_REPO=https://repo.maven.apache.org/maven2

RUN mkdir -p /opt/spark/extra-jars && \
    set -e && \
    echo "Downloading Spark dependencies..." && \

    curl -fL --retry 5 --retry-delay 5 --connect-timeout 30 \
      -o /opt/spark/extra-jars/spark-avro.jar \
      "${MAVEN_REPO}/org/apache/spark/spark-avro_${SCALA_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_VERSION}-${SPARK_VERSION}.jar" && \

    curl -fL --retry 5 --retry-delay 5 --connect-timeout 30 \
      -o /opt/spark/extra-jars/hadoop-aws.jar \
      "${MAVEN_REPO}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \

    curl -fL --retry 5 --retry-delay 5 --connect-timeout 30 \
      -o /opt/spark/extra-jars/aws-java-sdk-bundle.jar \
      "${MAVEN_REPO}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" && \

    curl -fL --retry 5 --retry-delay 5 --connect-timeout 30 \
      -o /opt/spark/extra-jars/deequ.jar \
      "${MAVEN_REPO}/com/amazon/deequ/deequ/${DEEQU_VERSION}/deequ-${DEEQU_VERSION}.jar" && \

    echo "Verifying downloaded JARs..." && \
    ls -lh /opt/spark/extra-jars && \
    test -f /opt/spark/extra-jars/spark-avro.jar && \
    test -f /opt/spark/extra-jars/hadoop-aws.jar && \
    test -f /opt/spark/extra-jars/aws-java-sdk-bundle.jar && \
    test -f /opt/spark/extra-jars/deequ.jar && \
    echo "All dependencies downloaded successfully"


ENV SPARK_EXTRA_CLASSPATH=/opt/spark/extra-jars/*

ENTRYPOINT ["/app/entrypoint.sh"]