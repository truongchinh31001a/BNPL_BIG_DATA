FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    datasets==2.18.0 \
    huggingface-hub==0.23.4 \
    requests==2.31.0 \
    pyarrow==16.1.0 \
    delta-spark==3.2.0 \
    kafka-python==2.0.2 \
    apache-airflow-providers-apache-spark==4.9.0
