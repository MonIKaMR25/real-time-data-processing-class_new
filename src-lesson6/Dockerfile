FROM python:3.13-slim

RUN pip install --no-cache-dir uv

# Keep the venv outside /workspace so the bind mount doesn't clobber it.
ENV UV_LINK_MODE=copy
RUN uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    VIRTUAL_ENV="/opt/venv" \
    PYTHONPATH="/workspace/src"

# confluent-kafka wheels bundle librdkafka — no system deps needed.
RUN uv pip install --python /opt/venv/bin/python \
    "confluent-kafka>=2.6.0" \
    "psycopg[binary]>=3.2.0"

WORKDIR /workspace
CMD ["sleep", "infinity"]
