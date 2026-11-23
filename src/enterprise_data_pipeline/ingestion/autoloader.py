from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

ENTITIES = (
    "customers",
    "orders",
    "order_items",
    "products",
)


def ingest_entity(
    spark: SparkSession,
    entity: str,
    catalog: str,
    schema: str,
) -> None:
    if entity not in ENTITIES:
        raise ValueError(f"Unsupported entity: {entity}")

    volume_path = f"/Volumes/{catalog}/{schema}/edp_storage"

    source_path = f"{volume_path}/landing/csv/{entity}/"
    schema_path = f"{volume_path}/schema/csv/{entity}/"
    checkpoint_path = f"{volume_path}/checkpoints/csv/{entity}/"

    target_table = f"{catalog}.{schema}.bronze_{entity}"

    # Read CSV files using Auto Loader
    entity_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .load(source_path)
    )

    # Add ingestion metadata
    entity_df = (
        entity_df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    # Write records into Bronze Delta table
    query = (
        entity_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()

    print(f"{entity} ingestion completed successfully.")


def ingest_all(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> None:
    for entity in ENTITIES:
        print(f"Starting Auto Loader ingestion: {entity}")

        ingest_entity(
            spark=spark,
            entity=entity,
            catalog=catalog,
            schema=schema,
        )

    print("All Bronze ingestion completed successfully.")
