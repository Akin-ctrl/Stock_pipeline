FROM python:3.12-slim

WORKDIR /app

# Minimal dependencies only if needed, currently none are required for the binary wheels

COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --retries 10 --timeout 1000 -r /app/requirements.txt

COPY app/ /app/

# Make entrypoint executable
RUN chmod +x /app/entrypoint.sh

ENV PYTHONPATH=/

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["tail", "-f", "/dev/null"]
