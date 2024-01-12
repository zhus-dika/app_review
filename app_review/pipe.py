import logging
from datetime import date, datetime

from etl.extract import extract_reviews_from_app_store, extract_reviews_from_google_play
from etl.load import load_app_store_topics_to_db, load_google_play_topics_to_db
from etl.transform import (
    transform_app_store_reviews_to_topics,
    transform_google_play_reviews_to_topics,
)
from etl.utils import (
    AppStoreEntity,
    GooglePlayEntity,
    create_psycopg2_connection,
    create_spark_session,
)
from psycopg2.extensions import connection as psycopg2_connection
from pyspark.sql import SparkSession


def pipe_app_store(session: SparkSession, connection: psycopg2_connection) -> None:
    dt = datetime.now()
    entity = AppStoreEntity(app_name="carx-street", app_id=1458863319, country="us", lang="en", dt=dt)
    filename = extract_reviews_from_app_store(session=session, entity=entity, start_dt=date(year=2023, month=1, day=1))
    lda_json = transform_app_store_reviews_to_topics(session=session, filename=filename, lang=entity.lang)
    load_app_store_topics_to_db(connection=connection, entity=entity, lda_json=lda_json)


def pipe_google_play(session: SparkSession, connection: psycopg2_connection) -> None:
    dt = datetime.now()
    entity = GooglePlayEntity(app_id="com.carxtech.sr", country="ru", lang="ru", dt=dt)
    filename = extract_reviews_from_google_play(session=session, entity=entity)
    lda_json = transform_google_play_reviews_to_topics(session=session, filename=filename, lang=entity.lang)
    load_google_play_topics_to_db(connection=connection, entity=entity, lda_json=lda_json)


def pipe():
    session = create_spark_session()
    conn = create_psycopg2_connection()
    try:
        with conn:
            pipe_app_store(session=session, connection=conn)
            pipe_google_play(session=session, connection=conn)
    finally:
        session.stop()
        conn.close()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    pipe()
