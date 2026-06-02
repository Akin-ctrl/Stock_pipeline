FROM apache/superset:4.1.2

USER root

RUN pip install --no-cache-dir psycopg2-binary

COPY superset/bootstrap_superset.py /app/bootstrap_superset.py

USER superset
