# webetl image: the CLI, the demo mock API and the Streamlit dashboard.
FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[postgres,s3,dashboard]"
COPY configs ./configs
COPY examples ./examples
COPY dashboard.py ./
ENTRYPOINT ["webetl"]
CMD ["--help"]
