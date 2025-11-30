FROM apache/airflow:2.8.2

USER root

RUN apt-get update && apt-get install -y --no-install-recommends default-jdk && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    requests==2.31.0 \
    apache-airflow-providers-apache-spark==4.7.0