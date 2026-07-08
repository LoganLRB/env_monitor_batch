FROM apache/airflow:2.9.3-python3.12

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Detect arch-specific JDK path (arm64 on Apple Silicon, amd64 on x86)
RUN JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(which java)")")") \
    && echo "JAVA_HOME=${JAVA_HOME}" >> /etc/environment \
    && echo "export JAVA_HOME=${JAVA_HOME}" >> /etc/profile \
    && ln -sfn "${JAVA_HOME}" /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Hadoop AWS jars: must match Hadoop version bundled with PySpark 3.5.x (Hadoop 3.4.x)
# hadoop-aws 3.4.x switched from AWS SDK v1 to v2 (software.amazon.awssdk:bundle)
RUN mkdir -p /opt/airflow/jars \
    && curl -fsSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar" \
       -o /opt/airflow/jars/hadoop-aws.jar \
    && curl -fsSL "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.26.13/bundle-2.26.13.jar" \
       -o /opt/airflow/jars/aws-sdk-bundle.jar \
    && chown -R airflow:root /opt/airflow/jars

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
