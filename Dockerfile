
FROM apache/airflow:2.6.1-python3.9

WORKDIR /opt/airflow

ENV PYTHONPATH ${PYTHONPATH}:/opt/airflow/dags/business-intelligence-infrastructure-python

ENV AIRFLOW_HOME=/opt/airflow

EXPOSE 8080

USER airflow

CMD ["webserver"]

