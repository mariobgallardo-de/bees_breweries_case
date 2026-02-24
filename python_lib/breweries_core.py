# python_lib/breweries_core.py
from __future__ import annotations

from typing import List, Dict
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


# -----------------------------
# DQ (genérico / reutilizável)
# -----------------------------
def validate_flag_threshold(df: DataFrame, flag_column: str, tolerance: float) -> None:
    """
    Valida % de FALSE para uma flag booleana.
    - df: DataFrame
    - flag_column: coluna booleana (True/False)
    - tolerance: máximo permitido de FALSE (0..1)
    """
    metrics = (
        df.select(
            F.count("*").alias("total"),
            F.sum(F.when(F.col(flag_column) == F.lit(False), 1).otherwise(0)).alias("invalid"),
        )
        .collect()[0]
    )

    total = metrics["total"]
    invalid = metrics["invalid"]
    ratio = (invalid / total) if total and total > 0 else 0.0

    if ratio > tolerance:
        raise Exception(
            f"SILVER DATA ALERT → {flag_column} exceeded tolerance "
            f"({ratio:.2%} > {tolerance:.2%})"
        )


def validate_non_empty(df: DataFrame, message: str = "Empty dataset") -> None:
    """Fail-fast se o DF estiver vazio."""
    if df.count() == 0:
        raise Exception(message)


def warn_null_keys(df: DataFrame, where_sql: str, warn_msg: str = "Null keys found") -> int:
    """
    Retorna quantidade de linhas que batem com a condição 'where_sql' e imprime WARN.
    Útil pra Gold (monitoramento sem quebrar pipeline).
    """
    n = df.filter(where_sql).count()
    if n > 0:
        print(f"[WARN] {warn_msg}: {n}")
    return n


# -----------------------------
# SILVER - Transformações puras
# -----------------------------
BUSINESS_COLUMNS: List[str] = [
    "name",
    "brewery_type",
    "address",
    "postal_code",
    "city",
    "state",
    "country",
    "latitude",
    "longitude",
    "phone",
    "website_url",
    "has_proper_encoding",
    "has_geolocation",
    "has_address",
    "has_website",
]


def standardize(df_raw: DataFrame) -> DataFrame:
    """
    Padroniza strings e tipos:
    - trim e initcap para country/city/state
    - state := coalesce(state_province, state)
    - address_1_canon := coalesce(address_1, street)
    - address := concat_ws(", ", address_1_canon, address_2, address_3)
    - phone normalizado
    - lat/long cast double e round(6)
    - ingestion_timestamp = current_timestamp
    """
    return (
        df_raw
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("country", F.initcap(F.trim(F.col("country"))))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn("state", F.initcap(F.trim(F.coalesce(F.col("state_province"), F.col("state")))))
        .withColumn("address_1_canon", F.coalesce(F.col("address_1"), F.col("street")))
        .withColumn("address", F.concat_ws(", ", F.col("address_1_canon"), F.col("address_2"), F.col("address_3")))
        .withColumn("phone", F.regexp_replace(F.trim(F.col("phone")), r"[^\d+]", ""))
        .withColumn("latitude", F.round(F.col("latitude").cast("double"), 6))
        .withColumn("longitude", F.round(F.col("longitude").cast("double"), 6))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


def add_quality_flags(df_standardized: DataFrame) -> DataFrame:
    """
    Flags de qualidade:
    - has_proper_encoding: True se não contém U+FFFD em colunas textuais
    - has_website: website_url não nulo
    - has_address: address não nulo
    - has_geolocation: lat/long não nulos
    """
    text_cols = ["name", "address", "country", "city", "state"]

    # garante boolean (não NULL) com coalesce
    condition = None
    for c in text_cols:
        expr = F.coalesce(F.col(c), F.lit("")).contains("\uFFFD")
        condition = expr if condition is None else (condition | expr)

    return (
        df_standardized
        .withColumn("has_proper_encoding", ~condition)
        .withColumn("has_website", F.col("website_url").isNotNull())
        .withColumn("has_address", F.col("address").isNotNull())
        .withColumn("has_geolocation", F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    )


def add_hash(df_flagged: DataFrame) -> DataFrame:
    """
    Hash determinístico baseado nas colunas de negócio para suportar MERGE incremental:
    - hash_merge = sha2(concat_ws("||", ...), 256)
    """
    return df_flagged.withColumn(
        "hash_merge",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in BUSINESS_COLUMNS]
            ),
            256
        ),
    )


def build_silver_df(df_hash: DataFrame) -> DataFrame:
    """Seleciona o schema final da Silver."""
    return df_hash.select(
        "id",
        "name",
        "brewery_type",
        "address",
        "postal_code",
        "city",
        "state",
        "country",
        "latitude",
        "longitude",
        "phone",
        "website_url",
        "has_proper_encoding",
        "has_geolocation",
        "has_address",
        "has_website",
        "ingestion_timestamp",
        "hash_merge",
    )


def silver_pipeline(df_raw: DataFrame) -> DataFrame:
    """Convenience: executa standardize -> flags -> hash -> select final."""
    return build_silver_df(add_hash(add_quality_flags(standardize(df_raw))))


# -----------------------------
# GOLD - Agregação pura
# -----------------------------
def build_gold(df_silver: DataFrame) -> DataFrame:
    """
    Gold = agregação por (country, state, city, brewery_type)
    Regras de negócio:
    - filtra has_proper_encoding == True
    - filtra has_address == True
    - brewery_count = countDistinct(id)
    - last_update = current_timestamp
    """
    return (
        df_silver
        .filter((F.col("has_proper_encoding") == F.lit(True)) & (F.col("has_address") == F.lit(True)))
        .groupBy("country", "state", "city", "brewery_type")
        .agg(F.countDistinct("id").alias("brewery_count"))
        .withColumn("last_update", F.current_timestamp())
    )