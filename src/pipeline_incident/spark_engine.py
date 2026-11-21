"""Set up Spark with the Delta connector already on the driver classpath."""

from pathlib import Path

from pyspark.sql import SparkSession


def session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("pipeline-incident-agent")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def smoke(spark: SparkSession, path: Path) -> dict[str, str]:
    spark.sql(
        "SELECT 'P100:1' AS event_id, CAST('10.00' AS DECIMAL(18,2)) AS amount"
    ).write.format("delta").save(str(path))
    rows = spark.read.format("delta").load(str(path)).collect()
    actual = [(row.event_id, str(row.amount)) for row in rows]
    if actual != [("P100:1", "10.00")]:
        raise RuntimeError(f"Delta smoke check returned {actual!r}")
    return {"event_id": "P100:1", "amount": "10.00", "spark": spark.version, "path": str(path)}
