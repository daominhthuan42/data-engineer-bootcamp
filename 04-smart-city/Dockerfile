FROM apache/spark:3.5.2

USER root

WORKDIR /opt/spark/work-dir

COPY requirements.docker.txt .

RUN pip install --no-cache-dir \
    -r requirements.docker.txt

USER spark
