FROM eclipse-temurin:17.0.18_8-jre-jammy@sha256:642d45bf22d3cb9face159181732ed9fa70873b2681e50445eff7d4785c176bb AS java
FROM ghcr.io/astral-sh/uv:0.13.9 AS uv
FROM python:3.11.15-slim-bookworm@sha256:d29f48a31a8b408ed19272ca1e7b10ebae13b240a27e862d3d4217c528e2e0c3

# Copy Java and uv into the Python image and set the Spark runtime paths.
COPY --from=java /opt/java/openjdk /opt/java/openjdk
COPY --from=uv /uv /usr/local/bin/uv
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/app/.venv/bin:/opt/java/openjdk/bin:$PATH \
    PYTHONPATH=/app/src \
    PYSPARK_PYTHON=/app/.venv/bin/python \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project --python /usr/local/bin/python \
    && command -v flock \
    && java -version
# Download and checksum the Delta connectors during the build.
COPY scripts/download_jars.py /tmp/download_jars.py
RUN python /tmp/download_jars.py
COPY . .
ENTRYPOINT ["bash", "/app/scripts/container"]
