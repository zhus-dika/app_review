FROM continuumio/miniconda3:23.10.0-1

RUN apt update && \
    apt install -y openjdk-11-jdk && \
    apt clean;

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

WORKDIR /app_review

COPY environment.yml .
RUN conda env create -f environment.yml
SHELL ["conda", "run", "--no-capture-output", "-n", "app_review", "/bin/bash", "-c"]

COPY poetry.lock pyproject.toml ./
RUN poetry config virtualenvs.create false
RUN poetry install --no-root --no-directory --no-cache --without dev

COPY app_review/webapp app_review/webapp
COPY app_review/etl app_review/etl
COPY app_review/dag.py app_review/dag.py
COPY configs/config.yaml configs/config.yaml

RUN echo "AUTH_ROLE_PUBLIC = 'Admin'" >> webserver_config.py
