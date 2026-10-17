"""PySpark implementation for million-row transaction processing.

PySpark imports stay inside functions so the original lightweight pipeline remains
usable without Java or optional dependencies.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_RECORD_COUNT = 1_000_000
SUPPORTED_COUNTRIES = ("IN", "US", "GB", "DE", "BR", "SG", "NG", "AE", "JP", "AU")
COUNTRY_CURRENCIES = {
    "IN": "INR",
    "US": "USD",
    "GB": "GBP",
    "DE": "EUR",
    "BR": "BRL",
    "SG": "SGD",
    "NG": "NGN",
    "AE": "AED",
    "JP": "JPY",
    "AU": "AUD",
}
FX_TO_USD = {
    "USD": 1.0,
    "INR": 0.012,
    "GBP": 1.27,
    "EUR": 1.09,
    "BRL": 0.20,
    "SGD": 0.74,
    "NGN": 0.00065,
    "AED": 0.2723,
    "JPY": 0.0067,
    "AUD": 0.66,
}
CATEGORIES = ("GROCERIES", "TRAVEL", "UTILITIES", "SHOPPING", "DINING", "SALARY", "TRANSFER")
CHANNELS = ("CARD", "BANK_TRANSFER", "UPI", "ACH", "WIRE", "MOBILE_WALLET")
STATUSES = ("COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "FAILED")


def _require_pyspark() -> tuple[Any, Any, Any]:
    try:
        from pyspark.sql import SparkSession, Window, functions as functions
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is not installed. Run: python -m pip install -r requirements-spark.txt"
        ) from exc
    return SparkSession, Window, functions


def create_spark_session(master: str = "local[*]", shuffle_partitions: int = 8) -> Any:
    SparkSession, _, _ = _require_pyspark()
    return (
        SparkSession.builder.appName("worldwide-transactions-pipeline")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _pick(values: tuple[str, ...], hash_column: Any, functions: Any) -> Any:
    choices = functions.array(*[functions.lit(value) for value in values])
    index = functions.pmod(hash_column, functions.lit(len(values))) + functions.lit(1)
    return functions.element_at(choices, index.cast("int"))


def generate_synthetic_transactions(spark: Any, count: int, seed: int, partitions: int) -> Any:
    """Generate records in Spark executors; no million-row Python list is created."""
    _, _, F = _require_pyspark()
    base = spark.range(0, count, numPartitions=partitions).withColumn(
        "source_row", F.col("id") + F.lit(1)
    )
    country_hash = F.xxhash64("id", F.lit(seed))
    counterparty_hash = F.xxhash64("id", F.lit(seed + 11))
    category_hash = F.xxhash64("id", F.lit(seed + 23))
    channel_hash = F.xxhash64("id", F.lit(seed + 37))
    status_hash = F.xxhash64("id", F.lit(seed + 51))

    country = _pick(SUPPORTED_COUNTRIES, country_hash, F)
    counterparty = _pick(SUPPORTED_COUNTRIES, counterparty_hash, F)
    category = _pick(CATEGORIES, category_hash, F)
    currency_map = F.create_map(
        *[item for pair in COUNTRY_CURRENCIES.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    amount = F.round(F.lit(10.0) + F.pow(F.rand(seed), F.lit(3.0)) * F.lit(50_000.0), 2)
    occurred_at = F.to_timestamp(
        F.from_unixtime(
            F.lit(1_767_225_600) + F.pmod(F.xxhash64("id", F.lit(seed + 71)), F.lit(15_552_000))
        )
    )

    raw = base.select(
        "source_row",
        F.concat(F.lit("WTX-"), F.lpad((F.col("id") + 1).cast("string"), 12, "0")).alias(
            "transaction_id"
        ),
        F.concat(
            F.lit("ACC-"),
            F.lpad((F.pmod(F.xxhash64("id"), F.lit(max(20, count // 8))) + 1).cast("string"), 8, "0"),
        ).alias("account_id"),
        occurred_at.alias("occurred_at"),
        F.when(F.pmod(F.col("source_row"), F.lit(997)) == 0, F.lit(None))
        .otherwise(country)
        .alias("country"),
        F.element_at(currency_map, country).alias("currency"),
        F.when(category == "SALARY", F.lit("CREDIT"))
        .when(F.pmod(F.xxhash64("id", F.lit(seed + 83)), F.lit(4)) == 0, F.lit("CREDIT"))
        .otherwise(F.lit("DEBIT"))
        .alias("direction"),
        _pick(CHANNELS, channel_hash, F).alias("channel"),
        category.alias("merchant_category"),
        F.when(category == "SALARY", F.lit("Global Payroll Ltd"))
        .otherwise(F.lit("Synthetic Global Merchant"))
        .alias("merchant_name"),
        F.when(F.pmod(F.col("source_row"), F.lit(991)) == 0, F.lit(-1.0))
        .otherwise(amount)
        .alias("amount"),
        _pick(STATUSES, status_hash, F).alias("status"),
        F.when(F.pmod(F.xxhash64("id", F.lit(seed + 97)), F.lit(4)) == 0, counterparty)
        .otherwise(country)
        .alias("counterparty_country"),
    )

    # One intentional duplicate verifies distributed de-duplication.
    duplicate = raw.orderBy("source_row").limit(1).withColumn("source_row", F.lit(count + 1))
    return raw.unionByName(duplicate)


def transform_transactions(raw: Any, account_hash_salt: str = "world-transactions-demo-v2") -> tuple[Any, Any]:
    _, Window, F = _require_pyspark()
    duplicate_window = Window.partitionBy("transaction_id").orderBy("source_row")
    ranked = raw.withColumn("duplicate_rank", F.row_number().over(duplicate_window))
    rejection_reason = (
        F.when(F.col("transaction_id").isNull(), F.lit("missing transaction_id"))
        .when(F.col("account_id").isNull(), F.lit("missing account_id"))
        .when(F.col("country").isNull(), F.lit("missing country"))
        .when(F.length("country") != 2, F.lit("country must be ISO alpha-2"))
        .when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("amount must be positive"))
        .when(F.col("duplicate_rank") > 1, F.lit("duplicate transaction_id"))
    )
    classified = ranked.withColumn("rejection_reason", rejection_reason)
    rejected = classified.filter(F.col("rejection_reason").isNotNull()).select(
        "source_row", "transaction_id", "rejection_reason"
    )

    fx_map = F.create_map(
        *[item for pair in FX_TO_USD.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    valid = classified.filter(F.col("rejection_reason").isNull())
    enriched = (
        valid.withColumn(
            "amount_usd",
            F.round(F.col("amount") * F.element_at(fx_map, F.col("currency")), 2),
        )
        .withColumn("is_cross_border", F.col("country") != F.col("counterparty_country"))
        .withColumn(
            "risk_score",
            F.when(F.col("amount_usd") >= 10_000, 45).otherwise(0)
            + F.when(F.col("is_cross_border"), 25).otherwise(0)
            + F.when(F.col("channel") == "WIRE", 15).otherwise(0)
            + F.when(F.col("status") == "FAILED", 20).otherwise(0),
        )
        .withColumn(
            "risk_band",
            F.when(F.col("risk_score") >= 60, "HIGH")
            .when(F.col("risk_score") >= 30, "MEDIUM")
            .otherwise("LOW"),
        )
        .withColumn(
            "risk_reasons",
            F.concat_ws(
                "|",
                F.when(F.col("amount_usd") >= 10_000, "HIGH_VALUE"),
                F.when(F.col("is_cross_border"), "CROSS_BORDER"),
                F.when(F.col("channel") == "WIRE", "WIRE_CHANNEL"),
                F.when(F.col("status") == "FAILED", "FAILED_STATUS"),
            ),
        )
        .withColumn(
            "account_hash",
            F.sha2(F.concat_ws(":", F.lit(account_hash_salt), F.col("account_id")), 256),
        )
        .withColumn("schema_version", F.lit("2.0"))
        .drop("account_id", "duplicate_rank", "rejection_reason")
    )
    return enriched, rejected


def run_spark_pipeline(
    spark: Any,
    count: int,
    output_dir: Path,
    seed: int = 42,
    partitions: int = 8,
) -> dict[str, Any]:
    _, _, F = _require_pyspark()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    raw = generate_synthetic_transactions(spark, count, seed, partitions).cache()
    clean, rejected = transform_transactions(raw)
    clean = clean.cache()
    rejected = rejected.cache()

    raw.write.mode("overwrite").partitionBy("country").parquet(str(output_dir / "raw_transactions"))
    clean.write.mode("overwrite").partitionBy("country").parquet(str(output_dir / "clean_transactions"))
    rejected.write.mode("overwrite").parquet(str(output_dir / "rejected_transactions"))

    totals_row = clean.agg(
        F.count("*").alias("loaded_count"),
        F.round(F.sum("amount_usd"), 2).alias("gross_volume_usd"),
        F.sum(F.col("is_cross_border").cast("long")).alias("cross_border_transactions"),
        F.sum(F.when(F.col("risk_band") == "HIGH", 1).otherwise(0)).alias("high_risk_transactions"),
    ).first()
    country_summary = clean.groupBy("country").agg(
        F.count("*").alias("transactions"), F.round(F.sum("amount_usd"), 2).alias("volume_usd")
    )
    category_summary = clean.groupBy("merchant_category").agg(
        F.count("*").alias("transactions"), F.round(F.sum("amount_usd"), 2).alias("volume_usd")
    )
    country_summary.coalesce(1).write.mode("overwrite").json(str(output_dir / "analytics_by_country"))
    category_summary.coalesce(1).write.mode("overwrite").json(str(output_dir / "analytics_by_category"))

    extracted_count = raw.count()
    rejected_count = rejected.count()
    summary = {
        "engine": "PySpark",
        "requested_count": count,
        "extracted_count": extracted_count,
        "loaded_count": int(totals_row["loaded_count"]),
        "rejected_count": rejected_count,
        "gross_volume_usd": float(totals_row["gross_volume_usd"] or 0),
        "cross_border_transactions": int(totals_row["cross_border_transactions"] or 0),
        "high_risk_transactions": int(totals_row["high_risk_transactions"] or 0),
        "partitions": partitions,
        "output_format": "partitioned parquet",
    }
    (output_dir / "spark_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    raw.unpersist()
    clean.unpersist()
    rejected.unpersist()
    return summary
