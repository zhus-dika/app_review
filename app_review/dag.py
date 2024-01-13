from datetime import date, datetime

from airflow.decorators import dag, task, task_group
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
from pendulum import today


@task
def extract_app_store_reviews(entity: AppStoreEntity, start_dt: date) -> str:
    session = create_spark_session()
    try:
        return extract_reviews_from_app_store(session=session, entity=entity, start_dt=start_dt)
    finally:
        session.stop()


@task
def transform_app_store_reviews(filename: str, lang: str) -> str:
    session = create_spark_session()
    try:
        return transform_app_store_reviews_to_topics(session=session, filename=filename, lang=lang)
    finally:
        session.stop()


@task
def load_app_store_topics(entity: AppStoreEntity, lda_json: str):
    conn = create_psycopg2_connection()
    try:
        with conn:
            load_app_store_topics_to_db(connection=conn, entity=entity, lda_json=lda_json)
    finally:
        conn.close()


@task_group
def app_store_etl(entity: AppStoreEntity, start_dt: date):
    filename = extract_app_store_reviews(entity=entity, start_dt=start_dt)
    lda_json = transform_app_store_reviews(filename=filename, lang=entity.lang)
    load_app_store_topics(entity=entity, lda_json=lda_json)


@task
def extract_google_play_reviews(entity: GooglePlayEntity) -> str:
    session = create_spark_session()
    try:
        return extract_reviews_from_google_play(session=session, entity=entity)
    finally:
        session.stop()


@task
def transform_google_play_reviews(filename: str, lang: str) -> str:
    session = create_spark_session()
    try:
        return transform_google_play_reviews_to_topics(session=session, filename=filename, lang=lang)
    finally:
        session.stop()


@task
def load_google_play_topics(entity: GooglePlayEntity, lda_json: str):
    conn = create_psycopg2_connection()
    try:
        with conn:
            load_google_play_topics_to_db(connection=conn, entity=entity, lda_json=lda_json)
    finally:
        conn.close()


@task_group
def google_play_etl(entity: GooglePlayEntity):
    filename = extract_google_play_reviews(entity=entity)
    lda_json = transform_google_play_reviews(filename=filename, lang=entity.lang)
    load_google_play_topics(entity=entity, lda_json=lda_json)


@dag(
    dag_id="app_review",
    default_args={"retries": 0},
    schedule="@daily",
    start_date=today("UTC"),
    catchup=False,
    tags=["big data", "spark", "hdfs"],
)
def pipe() -> None:
    dt = today()
    carx_street_app_store = AppStoreEntity(app_name="carx-street", app_id=1458863319, country="us", lang="en", dt=dt)
    carx_street_google_play = GooglePlayEntity(app_id="com.carxtech.sr", country="ru", lang="ru", dt=dt)
    _ = [
        app_store_etl(entity=carx_street_app_store, start_dt=datetime(year=2023, month=1, day=1)),
        google_play_etl(entity=carx_street_google_play),
    ]


pipe()
