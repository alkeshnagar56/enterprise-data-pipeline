
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

ENTITY_PRIMARY_KEYS = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "products": "product_id",
}

CDC_COLUMNS = {"_cdc_sequence", "_is_deleted"}
SYSTEM_COLUMNS = {
    "_cdc_sequence",
    "_is_deleted",
    "_ingestion_timestamp",
    "_source_file",
    "_rescued_data",
}


def prepare_silver_tables(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> None:
    """Add CDC tracking columns and enable Delta Change Data Feed."""

    for entity in ENTITY_PRIMARY_KEYS:
        table = f"{catalog}.{schema}.silver_{entity}"

        existing_columns = {
            field.name for field in spark.table(table).schema.fields
        }

        missing_columns = CDC_COLUMNS - existing_columns

        if missing_columns:
            column_definitions = ", ".join(
                f"{column} {'BIGINT' if column == '_cdc_sequence' else 'BOOLEAN'}"
                for column in sorted(missing_columns)
            )
            spark.sql(
                f"ALTER TABLE {table} ADD COLUMNS ({column_definitions})"
            )

        spark.sql(
            f"""
            UPDATE {table}
            SET
                _cdc_sequence = COALESCE(_cdc_sequence, -1),
                _is_deleted = COALESCE(_is_deleted, false)
            WHERE _cdc_sequence IS NULL OR _is_deleted IS NULL
            """
        )

        spark.sql(
            f"ALTER TABLE {table} "
            "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )

        print(f"CDC tracking and CDF enabled: {table}")


def apply_entity_changes(
    spark: SparkSession,
    events: DataFrame,
    catalog: str,
    schema: str,
    entity: str,
) -> None:
    """Apply the latest CDC events to one Silver Delta table."""

    primary_key = ENTITY_PRIMARY_KEYS[entity]
    target_table = f"{catalog}.{schema}.silver_{entity}"

    target_schema = spark.table(target_table).schema
    field_types = {
        field.name: field.dataType
        for field in target_schema.fields
    }

    business_fields = [
        field.name
        for field in target_schema.fields
        if field.name not in SYSTEM_COLUMNS
    ]

    payload_schema = StructType([
        field
        for field in target_schema.fields
        if field.name not in SYSTEM_COLUMNS
    ])

    parsed = (
        events.filter(F.col("entity") == entity)
        .withColumn(
            "_record",
            F.from_json(F.col("payload"), payload_schema),
        )
        .select(
            F.col("operation").alias("_operation"),
            F.col("event_sequence").cast("long").alias("_cdc_sequence"),
            F.col("primary_key").cast("string").alias("_event_key"),
            "_record.*",
        )
        .filter(
            F.col("_event_key").isNotNull()
            & F.col("_cdc_sequence").isNotNull()
            & F.col("_operation").isin("INSERT", "UPDATE", "DELETE")
        )
    )

    window = Window.partitionBy("_event_key").orderBy(
        F.col("_cdc_sequence").desc()
    )

    latest_events = (
        parsed.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    latest_events = latest_events.withColumn(
        primary_key,
        F.col("_event_key").cast(field_types[primary_key]),
    )

    source = latest_events.select(
        *[F.col(column) for column in business_fields],
        F.col("_cdc_sequence"),
        F.col("_operation"),
        (F.col("_operation") == "DELETE").alias("_is_deleted"),
    )

    source.createOrReplaceTempView(f"cdc_source_{entity}")

    update_assignments = {
        column: f"source.`{column}`"
        for column in business_fields
    }
    update_assignments["_cdc_sequence"] = "source._cdc_sequence"
    update_assignments["_is_deleted"] = "false"

    delete_assignments = {
        "_cdc_sequence": "source._cdc_sequence",
        "_is_deleted": "true",
    }

    insert_values = {
        column: f"source.`{column}`"
        for column in business_fields
    }
    insert_values["_cdc_sequence"] = "source._cdc_sequence"
    insert_values["_is_deleted"] = "source._is_deleted"

    target = DeltaTable.forName(spark, target_table)

    (
        target.alias("target")
        .merge(
            spark.table(f"cdc_source_{entity}").alias("source"),
            f"target.`{primary_key}` = source.`{primary_key}`",
        )
        .whenMatchedUpdate(
            condition=(
                "source._operation = 'DELETE' "
                "AND source._cdc_sequence > target._cdc_sequence"
            ),
            set=delete_assignments,
        )
        .whenMatchedUpdate(
            condition=(
                "source._operation IN ('INSERT', 'UPDATE') "
                "AND source._cdc_sequence > target._cdc_sequence"
            ),
            set=update_assignments,
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )

    print(f"Applied CDC events to {target_table}")


def apply_cdc_events(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> None:
    """Apply events from the Bronze CDC event table to Silver."""

    events_table = f"{catalog}.{schema}.bronze_cdc_events"

    if not spark.catalog.tableExists(events_table):
        print("No CDC event table found. Skipping CDC processing.")
        return

    events = spark.table(events_table)

    for entity in ENTITY_PRIMARY_KEYS:
        print(f"Processing CDC events for {entity}...")

        apply_entity_changes(
            spark=spark,
            events=events,
            catalog=catalog,
            schema=schema,
            entity=entity,
        )

    print("CDC processing completed.")
