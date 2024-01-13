from logging import info as log_info

import numpy as np
from pyLDAvis import PreparedData
from pyLDAvis import prepare as pyldavis_prepare
from pyspark.ml import Pipeline
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import IDF, CountVectorizer, StopWordsRemover
from pyspark.ml.feature import Tokenizer as PyTokenizer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array_remove, col, concat, regexp_replace, size, udf
from pyspark.sql.types import BooleanType
from sparknlp.annotator import (
    Chunker,
    LemmatizerModel,
    Normalizer,
    PerceptronModel,
    Tokenizer,
)
from sparknlp.base import DocumentAssembler, Finisher

from .consts import DEFAULT_SPARK_APP_NAME
from .utils import get_lda_config


def transform_app_store_reviews_to_topics(session: SparkSession, filename: str, lang: str) -> str:
    return _transform_reviews_to_topics(session=session, filename=filename, lang=lang, column="review")


def transform_google_play_reviews_to_topics(session: SparkSession, filename: str, lang: str) -> str:
    return _transform_reviews_to_topics(session=session, filename=filename, lang=lang, column="content")


def _transform_reviews_to_topics(session: SparkSession, filename: str, lang: str, column: str) -> str:
    use_sparknlp = session.sparkContext.getConf().get("spark.app.name") != DEFAULT_SPARK_APP_NAME
    log_info(f"transform_reviews_to_topics: filename={filename}, use_sparknlp={use_sparknlp}")

    df = session.read.parquet(filename)
    preprocessor = _preprocess_reviews_corpus_nlp if use_sparknlp else _preprocess_reviews_corpus
    df_processed, processed_column = preprocessor(df=df, column=column, lang=lang)

    return _prepare_lda_data(df=df_processed, column=processed_column)


def _preprocess_reviews_corpus(df: DataFrame, column: str, lang: str) -> tuple[DataFrame, str]:
    log_info("preprocess_reviews_corpus")

    df = df.select(regexp_replace(col(column), pattern=r"\p{Punct}", replacement=" ").alias("text"))

    tokenizer = PyTokenizer(inputCol="text", outputCol="tokens")
    df = tokenizer.transform(df).select(col("tokens"))

    stop_words_remover = StopWordsRemover(inputCol="tokens", outputCol="words", caseSensitive=False, locale=lang)
    df = stop_words_remover.transform(df).select(col("words"))

    is_not_short_udf = udf(lambda words: len(words) > 3, BooleanType())
    df = df.select(array_remove(col("words"), element="").alias("words")).filter(is_not_short_udf(col("words")))

    return df, "words"


def _preprocess_reviews_corpus_nlp(df: DataFrame, column: str, lang: str) -> tuple[DataFrame, str]:
    log_info("preprocess_reviews_corpus_nlp")

    doc_assembler = DocumentAssembler().setInputCol(column).setOutputCol("doc")
    tokenizer = Tokenizer().setInputCols(["doc"]).setOutputCol("tokenized")
    normalizer = Normalizer().setInputCols(["tokenized"]).setOutputCol("normalized").setLowercase(True)
    lemmatizer = LemmatizerModel().pretrained(lang=lang).setInputCols(["normalized"]).setOutputCol("lemmatized")
    pos_tagger = PerceptronModel().pretrained(lang=lang).setInputCols(["doc", "lemmatized"]).setOutputCol("pos")
    chunker = Chunker().setInputCols(["doc", "pos"]).setOutputCol("ngrams").setRegexParsers(["<JJ>+<NN>", "<NN>+<NN>"])
    finisher = Finisher().setInputCols(*["lemmatized", "ngrams"])

    pipeline = Pipeline(stages=[doc_assembler, tokenizer, normalizer, lemmatizer, pos_tagger, chunker, finisher])
    df_processed = pipeline.fit(df).transform(df)
    df_processed = df_processed.withColumn("processed", concat(col("finished_lemmatized"), col("finished_ngrams")))

    return df_processed, "processed"


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

    doc_lengths = np.array(df.select(size(col(column))).collect()).squeeze()
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
        start_index=1,
    ).to_json()
