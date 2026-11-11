FROM apache/airflow:2.9.0-python3.10

USER root

# Install system dependencies, including Docker CLI for triggering containerized Glue jobs
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        procps \
        docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Add airflow user to docker group if present
RUN usermod -aG root airflow || true

USER airflow

# Copy and install python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt
