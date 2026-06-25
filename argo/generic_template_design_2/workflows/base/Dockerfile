FROM python:3.11-slim

# Install dependencies
RUN pip install --no-cache-dir PyYAML==6.0

# Copy the normalizer script
COPY config-normalizer-entrypoint.py /entrypoint.py
RUN chmod +x /entrypoint.py

ENTRYPOINT ["/entrypoint.py"]
