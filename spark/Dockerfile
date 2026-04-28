# spark/Dockerfile
FROM apache/spark:3.5.1-python3

USER root
WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install dos2unix
RUN apt-get update && apt-get install -y --no-install-recommends dos2unix \
    && rm -rf /var/lib/apt/lists/*

# Copy Hadoop + Hive configs
COPY conf/core-site.xml /opt/spark/conf/
COPY conf/hive-site.xml /opt/spark/conf/

# Copy app
COPY app/cdc_processor.py .
COPY app/init_hive.py .
COPY app/entrypoint.sh .

RUN dos2unix /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# JAR versions (aligned)
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

RUN mkdir -p /opt/spark/extra-jars && \
    curl -fL -o /opt/spark/extra-jars/hadoop-aws.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fL -o /opt/spark/extra-jars/aws-java-sdk-bundle.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -fL -o /opt/spark/extra-jars/spark-avro.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.1/spark-avro_2.12-3.5.1.jar

ENV SPARK_EXTRA_CLASSPATH=/opt/spark/extra-jars/*

ENTRYPOINT ["/app/entrypoint.sh"]