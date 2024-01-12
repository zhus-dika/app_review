import time
from datetime import date, datetime
from logging import info as log_info

from app_store_scraper import AppStore
from google_play_scraper import Sort
from google_play_scraper import reviews as google_play_scraper_reviews
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .consts import MAX_APP_REVIEWS_COUNT
from .utils import (
    AppStoreEntity,
    GooglePlayEntity,
    get_hdfs_filename_of_app_store,
    get_hdfs_filename_of_google_play,
)


def extract_reviews_from_app_store(session: SparkSession, entity: AppStoreEntity, start_dt: date) -> str:
    assert 3 <= len(entity.app_name) <= 50 and len(entity.country) == 2 and len(entity.lang) == 2

    filename = get_hdfs_filename_of_app_store(entity)
    log_info(f"extract_reviews_from_app_store: filename={filename}, dt={entity.dt}")

    reviews = _fetch_reviews_from_app_store(entity=entity, start_dt=start_dt)
    _save_app_store_reviews_to_hdfs(session=session, reviews=reviews, filename=filename)

    return filename


def extract_reviews_from_google_play(session: SparkSession, entity: GooglePlayEntity) -> str:
    assert 3 <= len(entity.app_id) <= 50 and len(entity.country) == 2 and len(entity.lang) == 2

    filename = get_hdfs_filename_of_google_play(entity)
    log_info(f"extract_reviews_from_google_play: filename={filename}, dt={entity.dt}")

    reviews = _fetch_reviews_from_google_play(entity)
    _save_google_play_reviews_to_hdfs(session=session, reviews=reviews, filename=filename)

    return filename


def _fetch_reviews_from_app_store(entity: AppStoreEntity, start_dt: date) -> list[dict]:
    scraper = AppStore(country=entity.country, app_name=entity.app_name, app_id=entity.app_id)

    batch_size = 50
    while scraper.reviews_count < MAX_APP_REVIEWS_COUNT:
        count = scraper.reviews_count
        scraper.review(
            how_many=min(batch_size, MAX_APP_REVIEWS_COUNT - scraper.reviews_count),
            after=datetime(year=start_dt.year, month=start_dt.month, day=start_dt.day),
            sleep=1,
        )

        if count >= scraper.reviews_count:
            break

    return scraper.reviews


def _fetch_reviews_from_google_play(entity: GooglePlayEntity) -> list[dict]:
    reviews = []

    count, batch_size = 0, 200
    continuation_token = None
    while count < MAX_APP_REVIEWS_COUNT:
        batch, continuation_token = google_play_scraper_reviews(
            app_id=entity.app_id,
            lang=entity.lang,
            country=entity.country,
            sort=Sort.NEWEST,
            count=min(batch_size, MAX_APP_REVIEWS_COUNT - count),
            continuation_token=continuation_token,
        )

        reviews.extend(batch)
        count += len(batch)

        if len(batch) == 0 or continuation_token is None:
            break

        time.sleep(1)

    return reviews


def _save_app_store_reviews_to_hdfs(session: SparkSession, reviews: list[dict], filename: str) -> None:
    schema = StructType(
        [
            StructField(name="date", dataType=TimestampType(), nullable=False),
            StructField(name="isEdited", dataType=BooleanType(), nullable=False),
            StructField(name="rating", dataType=ByteType(), nullable=False),
            StructField(name="review", dataType=StringType(), nullable=False),
            StructField(name="title", dataType=StringType(), nullable=False),
            StructField(name="userName", dataType=StringType(), nullable=False),
        ]
    )
    _save_reviews_to_hdfs(session, reviews, schema, filename)


def _save_google_play_reviews_to_hdfs(session: SparkSession, reviews: list[dict], filename: str) -> None:
    schema = StructType(
        [
            StructField(name="reviewId", dataType=StringType(), nullable=False),
            StructField(name="userName", dataType=StringType(), nullable=False),
            StructField(name="content", dataType=StringType(), nullable=False),
            StructField(name="score", dataType=ByteType(), nullable=False),
            StructField(name="thumbsUpCount", dataType=IntegerType(), nullable=False),
            StructField(name="reviewCreatedVersion", dataType=StringType(), nullable=True),
            StructField(name="at", dataType=TimestampType(), nullable=False),
            StructField(name="appVersion", dataType=StringType(), nullable=True),
        ]
    )
    _save_reviews_to_hdfs(session, reviews, schema, filename)


def _save_reviews_to_hdfs(session: SparkSession, reviews: list[dict], schema: StructType, filename: str) -> None:
    log_info(f"save_reviews_to_hdfs: filename={filename}")
    data = [Row(**{name: review[name] for name in schema.fieldNames()}) for review in reviews]
    df = session.createDataFrame(data=data, schema=schema)
    df.write.parquet(path=filename, mode="overwrite")
