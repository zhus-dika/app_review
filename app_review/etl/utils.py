import os
from dataclasses import dataclass
from datetime import datetime
from logging import info as log_info

import sparknlp
from hydra import compose, initialize
from psycopg2 import connect
from psycopg2.extensions import connection as psycopg2_connection
from pyspark.sql import SparkSession

from .consts import CONFIG_NAME, CONFIG_PATH, DEFAULT_SPARK_APP_NAME


@dataclass
class AppStoreEntity:
    app_name: str
    app_id: int
    country: str
    lang: str
    dt: datetime


@dataclass
class GooglePlayEntity:
    app_id: str
    country: str
    lang: str
    dt: datetime


@dataclass
class LDAConfig:
    seed: int
    n_topics: int
    max_iter: int


def get_hdfs_filename_of_app_store(entity: AppStoreEntity) -> str:
    app_name, country, lang = entity.app_name.lower(), entity.country.lower(), entity.lang.lower()
    return f"app_store/{app_name}_{entity.app_id}_{country}_{lang}.parquet"


def get_hdfs_filename_of_google_play(entity: GooglePlayEntity) -> str:
    app_id, country, lang = entity.app_id.lower(), entity.country.lower(), entity.lang.lower()
    return f"google_play/{app_id}_{country}_{lang}.parquet"


def create_spark_session() -> SparkSession:
    with initialize(version_base=None, config_path=CONFIG_PATH):
        cfg = compose(config_name=CONFIG_NAME)["spark"]
        use_sparknlp, master, hadoop_user_name, hadoop_default_fs = (
            cfg["use_sparknlp"],
            cfg["master"],
            cfg["hadoop_user_name"],
            cfg["hadoop_default_fs"],
        )

    log_info(f"create_spark_session: use_sparknlp={use_sparknlp}, master={master}")
    log_info(f"                      hadoop_user_name={hadoop_user_name}, hadoop_default_fs={hadoop_default_fs}")

    os.environ["HADOOP_USER_NAME"] = hadoop_user_name

    if use_sparknlp:
        return sparknlp.start(params={"spark.hadoop.fs.defaultFS": hadoop_default_fs})

    return (
        SparkSession.builder.master(master)
        .appName(DEFAULT_SPARK_APP_NAME)
        .config("spark.hadoop.fs.defaultFS", hadoop_default_fs)
        .getOrCreate()
    )


def create_psycopg2_connection() -> psycopg2_connection:
    with initialize(version_base=None, config_path=CONFIG_PATH):
        cfg = compose(config_name=CONFIG_NAME)["db"]
        host, port, database, user, password = cfg["host"], cfg["port"], cfg["database"], cfg["user"], cfg["password"]

    log_info(f"create_psycopg2_connection: host={host}, port={port}, db={database}, user={user}")

    return connect(host=host, port=port, database=database, user=user, password=password)


def get_lda_config() -> LDAConfig:
    with initialize(version_base=None, config_path=CONFIG_PATH):
        cfg = compose(config_name=CONFIG_NAME)["lda"]
        seed, n_topics, max_iter = cfg["seed"], cfg["n_topics"], cfg["max_iter"]

    return LDAConfig(seed=seed, n_topics=n_topics, max_iter=max_iter)
