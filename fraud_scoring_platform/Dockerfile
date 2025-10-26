# Image Airflow + Spark + Great Expectations
# -----------------------------
# Dockerfile: Airflow + Spark + GE
# -----------------------------
# Base image : Apache Airflow officiel
FROM apache/airflow:2.6.3-python3.9

USER root

ARG SPARK_VERSION=3.4.1
ARG HADOOP_VERSION=3
ARG SPARK_BIGQUERY_CONNECTOR_VERSION=0.34.0
ARG SNOWFLAKE_CONNECTOR_VERSION=3.0.3
ARG POETRY_VERSION=1.5.1

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    SPARK_HOME=/opt/spark

# -----------------------------
# 1) System packages (Java + utils)
# -----------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    wget \
    curl \
    unzip \
    git \
    build-essential \
    libsnappy1v5 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# -----------------------------
# 2) Install PySpark and Python libs
# -----------------------------
# Pin versions if nécessaire
RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    great_expectations==0.16.20 \
    pandas \
    pyarrow \
    fastavro \
    google-cloud-bigquery \
    google-cloud-storage \
    snowflake-connector-python==${SNOWFLAKE_CONNECTOR_VERSION} \
    snowflake-sqlalchemy \
    fpdf \
    prometheus-client \
    elasticsearch \
    requests

# -----------------------------
# 3) Download Spark (standalone files for pyspark compat / jars)
#    and connectors (BigQuery / Snowflake)
# -----------------------------
RUN mkdir -p /opt/spark && \
    cd /opt/spark && \
    # spark is installed via pyspark wheel, but get connectors/jars into spark jars dir
    mkdir -p /opt/spark/jars

# Download BigQuery connector (spark-bigquery-with-dependencies)
RUN wget -q -O /opt/spark/jars/spark-bigquery-with-dependencies.jar \
    "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/${SPARK_BIGQUERY_CONNECTOR_VERSION}/spark-bigquery-with-dependencies_2.12-${SPARK_BIGQUERY_CONNECTOR_VERSION}.jar" || true

# Note: For Snowflake Spark connector you usually need two jars: snowflake-jdbc & spark-snowflake
# Keep them configurable: supply them via build ARG or mount them at runtime.
# Attempt to download commonly available versions (may need updates)
RUN wget -q -O /opt/spark/jars/snowflake-jdbc.jar "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.19/snowflake-jdbc-3.13.19.jar" || true && \
    wget -q -O /opt/spark/jars/spark-snowflake.jar "https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.11.0-spark_3.2/spark-snowflake_2.12-2.11.0-spark_3.2.jar" || true

# -----------------------------
# 4) Copy project skeleton (dags, jobs, configs, etc.)
# -----------------------------
# Assumes you'll build the image from repo root with these folders present
COPY dags/ /opt/airflow/dags/
COPY jobs/ /opt/airflow/jobs/
COPY configs/ /opt/airflow/config/
COPY requirements.txt /opt/airflow/requirements.txt
COPY great_expectations/ /opt/airflow/great_expectations/
COPY scripts/ /opt/airflow/scripts/
COPY reports/ /opt/airflow/reports/
COPY docker/ /opt/airflow/docker/

# Install any additional Python requirements (project specific)
RUN if [ -f /opt/airflow/requirements.txt ]; then pip install --no-cache-dir -r /opt/airflow/requirements.txt; fi

# -----------------------------
# 5) Permissions and user switch
# -----------------------------
RUN chown -R airflow: /opt/airflow
USER airflow

# -----------------------------
# 6) Environment variables (exemples)
# -----------------------------
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags \
    AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True \
    GE_HOME=/opt/airflow/great_expectations \
    SPARK_CONF_DIR=/opt/spark/conf \
    PYTHONUNBUFFERED=1

# Expose ports (Airflow webserver + Prometheus optional)
EXPOSE 8080 5555 8797

# -----------------------------
# 7) Entrypoint: reuse Airflow entrypoint
#    (the base image already provides entrypoint/cmd)
# -----------------------------
# If you want to run a custom command replace with entrypoint. By default the base image uses:
# ENTRYPOINT ["/usr/local/bin/dumb-init", "--"]
# CMD ["bash", "-lc", "airflow scheduler & airflow webserver"]
# We keep base behavior: when deploying use docker-compose or CLI to start webserver/scheduler.



# Construire l’image
# docker build -t data-platform-airflow-spark:latest .

# Lancer un conteneur (ex. pour debug, démarrer une shell) :
# docker run --rm -it \
#   -e "AIRFLOW__CORE__FERNET_KEY=your_fernet_key" \
#   -e "SNOWFLAKE_PASSWORD=..." \
#   -p 8080:8080 \
#   -v $(pwd)/dags:/opt/airflow/dags \
#   -v $(pwd)/jobs:/opt/airflow/jobs \
#   data-platform-airflow-spark:latest /bin/bash
