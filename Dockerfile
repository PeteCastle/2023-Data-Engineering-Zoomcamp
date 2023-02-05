# FROM python:3.9.1
# RUN pip install pandas sqlalchemy psycopg2 pyarrow fastparquet wget

# WORKDIR /app
# COPY ingest_data.py ingest_data.py

# ENTRYPOINT ["python" , "ingest_data.py"]

FROM prefecthq/prefect:2.7.7-python3.9

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
COPY resources/datasets /opt/prefect/resources/datasets