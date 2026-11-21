# Airflow 3 with webetl installed in its own virtualenv, so their dependencies never clash.
# The DAGs call /opt/webetl-venv/bin/webetl.
FROM apache/airflow:3.4.0

USER root
COPY pyproject.toml README.md /opt/webetl/
COPY src /opt/webetl/src
RUN python -m venv /opt/webetl-venv \
 && /opt/webetl-venv/bin/pip install --no-cache-dir "/opt/webetl[postgres,s3]" \
 && mkdir -p /data/lake /data/state \
 && chown -R airflow:0 /data
USER airflow
