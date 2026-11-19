
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name
)


def ingest_orders(
    spark: SparkSession,
    catalog: str,
    schema: str
):
    source_path = (
        "/Volumes/project_work/dev/edp_storage/landing/orders/"
    )

    schema_path = (
        "/Volumes/project_work/dev/edp_storage/schema/orders/"
    )

    checkpoint_path = (
        "/Volumes/project_work/dev/edp_storage/checkpoints/orders/"
    )

    target_table = f"{catalog}.{schema}.bronze_orders"

    # Read incoming JSON files using Auto Loader
    orders_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
    )

    # Add ingestion metadata
    orders_df = (
        orders_df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    # Write data into Bronze Delta table
    query = (
        orders_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()

    print("Orders ingestion completed successfully.")
