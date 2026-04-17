FROM apache/airflow:3.1.0-python3.12

COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt

USER airflow