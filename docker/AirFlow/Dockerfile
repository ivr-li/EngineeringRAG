FROM apache/airflow:3.2.0-python3.12

COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir uv

ENV UV_HTTP_TIMEOUT=300
ENV UV_LINK_MODE=copy
ENV PATH="/home/airflow/.local/bin:$PATH"

RUN uv pip install --no-cache -r /requirements.txt
