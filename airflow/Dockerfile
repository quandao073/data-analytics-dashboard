FROM apache/airflow:2.10.5

ARG SPARK_VERSION=3.5.5
ARG HADOOP_VERSION=3

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     openjdk-17-jre-headless \
     curl \
  && mkdir -p /opt/spark \
  && curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
     | tar zx -C /opt/spark --strip-components=1 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

COPY ./spark-jars/ $SPARK_HOME/jars/

USER airflow

RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    apache-airflow-providers-apache-spark==5.1.1 \
    kafka-python \
    requests \
    pandas \
    pyarrow
