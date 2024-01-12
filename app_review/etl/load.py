from logging import info as log_info

from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.sql import SQL, Identifier, Literal

from .consts import APP_STORE_TOPICS_TABLE, GOOGLE_PLAY_TOPICS_TABLE
from .utils import AppStoreEntity, GooglePlayEntity


def load_app_store_topics_to_db(connection: psycopg2_connection, entity: AppStoreEntity, lda_json: str) -> None:
    log_info("load_app_store_topics_to_db")

    query = SQL(
        """
        insert into {table} (app_name, app_id, country, lang, lda_data, update_dt)
        values ({app_name}, {app_id}, {country}, {lang}, {lda_data}, {update_dt})
        on conflict (app_name, app_id, country)
        do update set
            lang = {lang},
            lda_data = {lda_data},
            update_dt = {update_dt}
        """
    ).format(
        table=Identifier(*APP_STORE_TOPICS_TABLE.split(".")),
        app_name=Literal(entity.app_name),
        app_id=Literal(str(entity.app_id)),
        country=Literal(entity.country),
        lang=Literal(entity.lang),
        lda_data=Literal(lda_json),
        update_dt=Literal(entity.dt),
    )

    with connection.cursor() as cur:
        cur.execute(query)


def load_google_play_topics_to_db(connection: psycopg2_connection, entity: GooglePlayEntity, lda_json: str) -> None:
    log_info("load_google_play_topics_to_db")

    query = SQL(
        """
        insert into {table} (app_id, country, lang, lda_data, update_dt)
        values ({app_id}, {country}, {lang}, {lda_data}, {update_dt})
        on conflict (app_id, country)
        do update set
            lang = {lang},
            lda_data = {lda_data},
            update_dt = {update_dt}
        """
    ).format(
        table=Identifier(*GOOGLE_PLAY_TOPICS_TABLE.split(".")),
        app_id=Literal(entity.app_id),
        country=Literal(entity.country),
        lang=Literal(entity.lang),
        lda_data=Literal(lda_json),
        update_dt=Literal(entity.dt),
    )

    with connection.cursor() as cur:
        cur.execute(query)
