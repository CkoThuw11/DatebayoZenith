# Use an official OpenJDK 8 (Required for Hive 3)
FROM eclipse-temurin:8-jre-focal

# 1. Set environment variables
ENV HIVE_HOME=/opt/hive
ENV HIVE_VERSION=3.1.3
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_VERSION=3.3.4
ENV PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
ENV HIVE_CONF_DIR=$HIVE_HOME/conf
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# 2. Setup Directories and User
USER root
RUN mkdir -p $HIVE_HOME $HADOOP_HOME /var/lib/hive && \
    groupadd -r hive && \
    useradd -r -g hive -d $HIVE_HOME -s /sbin/nologin hive

# 3. Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends tini curl netcat-openbsd ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 4. DOWNLOAD & EXTRACT Hive and Hadoop (Replaces local COPY)
RUN curl -L https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz -o /tmp/hive.tar.gz && \
    tar -xzvf /tmp/hive.tar.gz -C /opt && \
    mv /opt/apache-hive-${HIVE_VERSION}-bin/* $HIVE_HOME && \
    curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -o /tmp/hadoop.tar.gz && \
    tar -xzvf /tmp/hadoop.tar.gz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION}/* $HADOOP_HOME && \
    rm /tmp/*.tar.gz

# 5. 🔥 THE GUAVA FIX (Critical: Hive 3 vs Hadoop 3 conflict)
RUN rm $HIVE_HOME/lib/guava-19.0.jar && \
    cp $HADOOP_HOME/share/hadoop/common/lib/guava-*.jar $HIVE_HOME/lib/

# 6. DOWNLOAD JARs for Postgres and S3A (MinIO)
# Fetching specific versions compatible with Hadoop 3.3.4
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.2.24.jar -o $HIVE_HOME/lib/postgresql-42.2.24.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o $HIVE_HOME/lib/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -o $HIVE_HOME/lib/hadoop-aws-${HADOOP_VERSION}.jar

# 7. HARDCODED CONFIGURATION (Requires local files in the 'hive/' folder)
COPY --chown=hive:hive conf/hive-site.xml $HIVE_CONF_DIR/hive-site.xml
COPY --chown=hive:hive conf/core-site.xml $HADOOP_CONF_DIR/core-site.xml
COPY --chown=hive:hive entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && sed -i 's/\r$//' /entrypoint.sh

# 8. Set ownership and User
RUN chown -R hive:hive $HIVE_HOME $HADOOP_HOME /var/lib/hive
USER hive

EXPOSE 9083

ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]