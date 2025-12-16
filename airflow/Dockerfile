FROM apache/airflow:2.8.0

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install Python dependencies
COPY app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set PYTHONPATH to include Stock_pipeline directory
ENV PYTHONPATH=/Stock_pipeline

