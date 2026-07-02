FROM apache/airflow:2.9.3-python3.12

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN mkdir -p /opt/airflow/data/bronze \
             /opt/airflow/data/silver \
             /opt/airflow/data/gold \
    && chown -R airflow:root /opt/airflow/data

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
