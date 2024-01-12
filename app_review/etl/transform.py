from logging import info as log_info

import numpy as np

# from emoji import demojize
from pyLDAvis import PreparedData
from pyLDAvis import prepare as pyldavis_prepare
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import IDF, CountVectorizer, StopWordsRemover, Tokenizer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array_remove, col, regexp_replace, size, udf
from pyspark.sql.types import BooleanType

from .utils import get_lda_config


def transform_app_store_reviews_to_topics(session: SparkSession, filename: str, lang: str) -> str:
    return _transform_reviews_to_topics(session=session, filename=filename, lang=lang, column="review")


def transform_google_play_reviews_to_topics(session: SparkSession, filename: str, lang: str) -> str:
    return _transform_reviews_to_topics(session=session, filename=filename, lang=lang, column="content")


def _transform_reviews_to_topics(session: SparkSession, filename: str, lang: str, column: str) -> str:
    log_info(f"transform_reviews_to_topics: filename={filename}")
    df = session.read.parquet(filename)
    df_corpus = _preprocess_reviews_corpus(df=df, column=column, lang=lang)
    return _prepare_lda_data(df=df_corpus, column="words")


def _preprocess_reviews_corpus(df: DataFrame, column: str, lang: str) -> DataFrame:
    # @udf(returnType=StringType())
    # def replace_emoji_udf(text: Column) -> str:
    #    return demojize(str(text), language=lang, delimiters=(" _", "_ "))

    log_info("preprocess_reviews_corpus")

    df = df.select(regexp_replace(col(column), pattern=r"\p{Punct}", replacement=" ").alias("text"))
    # df = df.select(replace_emoji_udf(col("text")).alias("text"))

    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
    df = tokenizer.transform(df).select(col("tokens"))

    stop_words_remover = StopWordsRemover(inputCol="tokens", outputCol="words", caseSensitive=False, locale=lang)
    df = stop_words_remover.transform(df).select(col("words"))

    is_not_empty_udf = udf(lambda words: len(words) > 0, BooleanType())
    df = df.select(array_remove(col("words"), element="").alias("words")).filter(is_not_empty_udf(col("words")))
    # TODO: remove short words and small or large docs

    return df


def _prepare_lda_data(df: DataFrame, column: str) -> PreparedData:
    cfg = get_lda_config()
    log_info(f"prepare_lda_data: seed={cfg.seed}, n_topics={cfg.n_topics}, max_iter={cfg.max_iter}")

    cv = CountVectorizer(inputCol=column, outputCol="tf").fit(df)
    df_tf = cv.transform(df)

    idf = IDF(inputCol="tf", outputCol="tf_idf").fit(df_tf)
    df_tf_idf = idf.transform(df_tf)

    lda = LDA(k=cfg.n_topics, maxIter=cfg.max_iter, featuresCol="tf_idf", seed=cfg.seed).fit(df_tf_idf)

    n_terms = lda.vocabSize()
    topic_term_dists = lda.topicsMatrix().toArray().transpose()
    assert topic_term_dists.shape == (cfg.n_topics, n_terms)

    n_docs = idf.numDocs
    assert n_docs == df.count()

    topic_dists = lda.transform(df_tf_idf).select(col("topicDistribution")).collect()
    doc_topic_dists = np.array(topic_dists)[:, 0, :]
    assert doc_topic_dists.shape == (n_docs, cfg.n_topics)

    doc_lengths = np.array(df.select(size(col("words"))).collect()).squeeze()
    assert doc_lengths.shape == (n_docs,) and (doc_lengths > 0).all()

    vocab, term_frequency = cv.vocabulary, idf.docFreq
    assert len(vocab) == n_terms and len(term_frequency) == n_terms

    return pyldavis_prepare(
        topic_term_dists=topic_term_dists,
        doc_topic_dists=doc_topic_dists,
        doc_lengths=doc_lengths,
        vocab=vocab,
        term_frequency=term_frequency,
        sort_topics=False,
        start_index=0,
    ).to_json()
