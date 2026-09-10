FROM apache/airflow:2.6.0

USER root

# Install system dependencies for pymssql
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        freetds-dev \
        build-essential \
        python3-dev \
        libkrb5-dev \
        libssl-dev \
        curl \
        ca-certificates \
        && rm -rf /var/lib/apt/lists/*

# --- Spark support -----------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-11-jre-headless procps && rm -rf /var/lib/apt/lists/*
ARG TARGETARCH
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
RUN mkdir -p /opt/airflow/jars && \
    curl -L -o /opt/airflow/jars/postgresql-42.7.3.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    curl -L -o /opt/airflow/jars/mysql-connector-j-8.4.0.jar \
      https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar && \
    curl -L -o /opt/airflow/jars/mssql-jdbc-12.6.1.jre11.jar \
      https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.6.1.jre11/mssql-jdbc-12.6.1.jre11.jar
# -----------------------------------------------------------------------------

# Switch back to airflow user
USER airflow

# Copy requirements.txt
COPY requirements.txt /

# Upgrade pip and install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r /requirements.txt
