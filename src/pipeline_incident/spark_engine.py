"""Set up Spark with the Delta connector already on the driver classpath."""

from dataclasses import astuple
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline_incident.contracts import Payment
from pipeline_incident.deliveries import MAX_ROWS, PipelineError, Workspace

SILVER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("payment_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("revision", LongType(), False),
        StructField("occurred_at", TimestampType(), False),
        StructField("payment_date", DateType(), False),
        StructField("status", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("amount", DecimalType(18, 2), False),
        StructField("channel", StringType(), True),
        StructField("payload_hash", StringType(), False),
        StructField("delivery_id", StringType(), False),
    ]
)
METADATA = "spark.databricks.delta.commitInfo.userMetadata"


def session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("pipeline-incident-agent")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def smoke(spark: SparkSession, path: Path) -> dict[str, str]:
    spark.sql("SELECT 'P100:1' AS event_id, CAST('10.00' AS DECIMAL(18,2)) AS amount").write.format(
        "delta"
    ).save(str(path))
    rows = spark.read.format("delta").load(str(path)).collect()
    actual = [(row.event_id, str(row.amount)) for row in rows]
    if actual != [("P100:1", "10.00")]:
        raise RuntimeError(f"Delta smoke check returned {actual!r}")
    return {"event_id": "P100:1", "amount": "10.00", "spark": spark.version, "path": str(path)}


class Engine:
    def __init__(self, spark: SparkSession, workspace: Workspace):
        self.spark = spark
        self.workspace = workspace

    def capture(self) -> None:
        source = self.spark.readStream.option("maxFilesPerTrigger", 1).text(
            str(self.workspace.inbox)
        )
        bronze = source.select(
            F.col("value").alias("raw_line"),
            F.input_file_name().alias("source_path"),
        ).withColumn("delivery_id", F.regexp_extract("source_path", r"/([^/]+)\.jsonl$", 1))
        query = (
            bronze.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", str(self.workspace.checkpoint))
            .trigger(availableNow=True)
            .start(str(self.workspace.bronze))
        )
        if not query.awaitTermination(150):
            query.stop()
            raise PipelineError(
                "STREAM_TIMEOUT", "Bronze capture timed out. Its checkpoint is retained."
            )

    def bronze_lines(self, delivery_id: str) -> list[str]:
        if not (self.workspace.bronze / "_delta_log").exists():
            return []
        records = (
            self.spark.read.format("delta")
            .load(str(self.workspace.bronze))
            .where(F.col("delivery_id") == delivery_id)
            .select("raw_line")
            .limit(MAX_ROWS + 1)
            .collect()
        )
        return [cast(str, record.raw_line) for record in records]

    def ensure_silver(self) -> None:
        (
            DeltaTable.createIfNotExists(self.spark)
            .location(str(self.workspace.silver))
            .addColumns(SILVER_SCHEMA)
            .execute()
        )

    @property
    def silver(self) -> DeltaTable:
        return DeltaTable.forPath(self.spark, str(self.workspace.silver))

    @property
    def version(self) -> int:
        commit = self.silver.history(1).select("version").first()
        if commit is None:
            raise PipelineError("NO_SILVER_HISTORY", "The Silver table has no recorded commits.")
        return int(commit.version)

    def history(self, ids: set[str]) -> list[dict[str, Any]]:
        if not ids:
            return []
        limit = MAX_ROWS * 5
        records = (
            self.silver.toDF()
            .where(F.col("payment_id").isin(sorted(ids)))
            .select(
                "payment_id",
                "revision",
                "merchant_id",
                "payment_date",
                "payload_hash",
                "delivery_id",
            )
            .limit(limit + 1)
            .collect()
        )
        if len(records) > limit:
            raise PipelineError(
                "HISTORY_LIMIT", "Affected payment history exceeds the local replay limit."
            )
        return [record.asDict() for record in records]

    def payment_frame(self, payments: list[Payment], delivery_id: str) -> DataFrame:
        records: list[tuple[object, ...]] = []
        for payment in payments:
            (
                event_id,
                payment_id,
                merchant,
                revision,
                occurred,
                day,
                status,
                currency,
                amount,
                channel,
            ) = astuple(payment)
            records.append(
                (
                    event_id,
                    payment_id,
                    merchant,
                    revision,
                    datetime.fromisoformat(occurred),
                    date.fromisoformat(day),
                    status,
                    currency,
                    Decimal(amount),
                    channel,
                    payment.digest,
                    delivery_id,
                )
            )
        return self.spark.createDataFrame(records, SILVER_SCHEMA)

    def insert(self, payments: list[Payment], delivery_id: str, metadata: str) -> None:
        self.spark.conf.set(METADATA, metadata)
        try:
            (
                self.silver.alias("history")
                .merge(
                    self.payment_frame(payments, delivery_id).alias("incoming"),
                    "history.payment_id = incoming.payment_id AND history.revision = incoming.revision",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        finally:
            self.spark.conf.unset(METADATA)

    def commit_versions(self, metadata: str) -> list[int]:
        commits = (
            self.silver.history()
            .where(F.col("userMetadata") == metadata)
            .select("version")
            .collect()
        )
        return [int(commit.version) for commit in commits]

    def committed_rows(self, delivery_id: str, version: int, limit: int) -> list[list[Any]]:
        records = (
            self.spark.read.format("delta")
            .option("versionAsOf", version)
            .load(str(self.workspace.silver))
            .where(F.col("delivery_id") == delivery_id)
            .select("payment_id", "revision", "payload_hash")
            .limit(limit + 1)
            .collect()
        )
        return sorted(
            [record.payment_id, record.revision, record.payload_hash] for record in records
        )

    def totals(self, additional: list[Payment] | None = None) -> dict[str, Any]:
        history = self.silver.toDF()
        if additional:
            history = history.unionByName(self.payment_frame(additional, "PREVIEW"))
        window = Window.partitionBy("payment_id").orderBy(F.col("revision").desc())
        current = (
            history.withColumn("rank", F.row_number().over(window)).where("rank = 1").drop("rank")
        )
        current.createOrReplaceTempView("payment_current")
        for status, view in (
            ("CAPTURED", "payments_daily_by_merchant"),
            ("REFUNDED", "refunds_daily_by_merchant"),
        ):
            self.spark.sql(
                f"CREATE OR REPLACE TEMP VIEW {view} AS SELECT merchant_id, payment_date, "
                f"SUM(amount) AS amount FROM payment_current WHERE status = '{status}' "
                "GROUP BY merchant_id, payment_date"
            )
        captured = self.spark.table("payments_daily_by_merchant").agg(
            F.sum("amount").alias("captured")
        )
        refunded = self.spark.table("refunds_daily_by_merchant").agg(
            F.sum("amount").alias("refunded")
        )
        balances = captured.crossJoin(refunded).first()
        if balances is None:
            raise PipelineError("NO_TOTALS", "The serving view totals could not be read.")
        return {
            "history_rows": history.count(),
            "current_payments": current.count(),
            "captured": str(balances.captured or Decimal("0.00")),
            "refunded": str(balances.refunded or Decimal("0.00")),
        }
