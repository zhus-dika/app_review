from typing import Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from psycopg2.sql import SQL, Composed, Identifier, Literal
from pyLDAvis import prepared_data_to_html

from ..etl.consts import APP_STORE_TOPICS_TABLE, GOOGLE_PLAY_TOPICS_TABLE
from ..etl.utils import create_psycopg2_connection

app = FastAPI()


class _PreparedData:
    def __init__(self, json_data: str):
        self._json_data = json_data

    def to_json(self) -> str:
        return self._json_data


def _execute_query(query: Composed) -> list[tuple[Any, ...]]:
    conn = create_psycopg2_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/", response_class=HTMLResponse)
async def root() -> HTMLResponse:
    content = """
    <!DOCTYPE html>
    <html>
        <head>
            <meta charset="utf-8">
            <title>stores</title>
        </head>
        <body>
            <ul>
                <li><a href="/app_store">app store</a></li>
                <li><a href="/google_play">google play</a></li>
            </ul>
        </body>
    </html>
    """
    return HTMLResponse(content=content, status_code=200)


@app.get("/app_store", response_class=HTMLResponse)
async def app_store():
    def href(row: tuple[Any, ...]) -> str:
        return f'<li><a href="/app_store/{row[0]}/{row[1]}/{row[2]}">{row[0]} [{row[2]}]</a></li>'

    query = SQL("select app_name, app_id, country from {} order by app_name, app_id, country").format(
        Identifier(*APP_STORE_TOPICS_TABLE.split("."))
    )
    apps = _execute_query(query)

    content = f"""
    <!DOCTYPE html>
    <html>
        <head>
            <meta charset="utf-8">
            <title>app store</title>
        </head>
        <body>
            <ul>
                {"".join(href(row) for row in apps)}
                <li><a href="/">..</a></li>
            </ul>
        </body>
    </html>
    """
    return HTMLResponse(content=content, status_code=200)


@app.get("/google_play", response_class=HTMLResponse)
async def google_play():
    def href(row: tuple[Any, ...]) -> str:
        return f'<li><a href="/google_play/{row[0]}/{row[1]}">{row[0]} [{row[1]}]</a></li>'

    query = SQL("select app_id, country from {} order by app_id, country").format(
        Identifier(*GOOGLE_PLAY_TOPICS_TABLE.split("."))
    )
    apps = _execute_query(query)

    content = f"""
    <!DOCTYPE html>
    <html>
        <head>
            <meta charset="utf-8">
            <title>google play</title>
        </head>
        <body>
            <ul>
                {"".join(href(row) for row in apps)}
                <li><a href="/">..</a></li>
            </ul>
        </body>
    </html>
    """
    return HTMLResponse(content=content, status_code=200)


@app.get("/app_store/{app_name}/{app_id}/{country}", response_class=HTMLResponse)
async def app_store_topics(app_name: str, app_id: str, country: str) -> HTMLResponse:
    query = SQL(
        """
        select lda_data
        from {table}
        where app_name = {app_name} and app_id = {app_id} and country = {country}
        """
    ).format(
        table=Identifier(*APP_STORE_TOPICS_TABLE.split(".")),
        app_name=Literal(app_name),
        app_id=Literal(app_id),
        country=Literal(country),
    )

    json_data = _execute_query(query)[0][0]
    data = _PreparedData(json_data)
    content = prepared_data_to_html(data=data)
    return HTMLResponse(content=content, status_code=200)


@app.get("/google_play/{app_id}/{country}", response_class=HTMLResponse)
async def google_play_topics(app_id: str, country: str) -> HTMLResponse:
    query = SQL(
        """
        select lda_data
        from {table}
        where app_id = {app_id} and country = {country}
        """
    ).format(
        table=Identifier(*GOOGLE_PLAY_TOPICS_TABLE.split(".")),
        app_id=Literal(app_id),
        country=Literal(country),
    )

    json_data = _execute_query(query)[0][0]
    data = _PreparedData(json_data)
    content = prepared_data_to_html(data=data)
    return HTMLResponse(content=content, status_code=200)
