FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DATA_DIR=/data
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir . && useradd --uid 10001 --create-home retention \
    && mkdir /data && chown retention:retention /data
USER 10001:10001
ENTRYPOINT ["retentionbot"]
CMD ["observer"]
