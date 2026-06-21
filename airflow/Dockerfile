FROM apache/airflow:2.10.4-python3.12

# Set PYTHONPATH to include Stock_pipeline directory
ENV PYTHONPATH=/Stock_pipeline

# Copy and install Python dependencies
COPY airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir --retries 10 --timeout 1000 -r /tmp/airflow-requirements.txt

