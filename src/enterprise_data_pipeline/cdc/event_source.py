
import json
from datetime import UTC, datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ENTITY_PRIMARY_KEYS = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "products": "product_id",
}

SYSTEM_COLUMNS = {
    "_cdc_sequence",
    "_is_deleted",
    "_ingestion_timestamp",
    "_source_file",
    "_rescued_data",
}

EVENT_SCHEMA = StructType([
    StructField("entity", StringType(), False),
    StructField("operation", StringType(), False),
    StructField("event_sequence", LongType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("primary_key", StringType(), False),
    StructField("payload", StringType(), True),
])


def build_updated_payload(row, primary_key: str) -> str:
    """Create a visibly changed record for a simulated UPDATE event."""

    payload = row.asDict(recursive=True)

    candidates = [
        name
        for name, value in payload.items()
        if name != primary_key
        and not name.lower().endswith("_id")
        and name.lower() != "status"
        and isinstance(value, str)
    ]

    if candidates:
        column = candidates[0]
        payload[column] = f"{payload[column]}_CDC_UPDATED"

    elif isinstance(payload.get("status"), str):
        payload["status"] = (
            "PROCESSING" if payload["status"] != "PROCESSING" else "PENDING"
        )

    else:
        candidates = [
            name
            for name, value in payload.items()
            if name != primary_key
            and isinstance(value, (int, float, Decimal))
        ]

        if not candidates:
            raise ValueError(
                f"No suitable field found to demonstrate UPDATE for {primary_key}"
            )

        column = candidates[0]
        payload[column] += 1

    return json.dumps(payload, default=str)


def create_demo_cdc_events(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> None:
    """Append a fresh batch of simulated CDC events."""

    events_table = f"{catalog}.{schema}.bronze_cdc_events"

    if spark.catalog.tableExists(events_table):
        latest_sequence = (
            spark.table(events_table)
            .agg(F.max("event_sequence").alias("max_sequence"))
            .first()["max_sequence"]
        )
        event_sequence = int(latest_sequence or 1000) + 1
    else:
        event_sequence = 1001

    records = []
    event_timestamp = datetime.now(UTC).replace(tzinfo=None)

    for entity, primary_key in ENTITY_PRIMARY_KEYS.items():
        silver_table = f"{catalog}.{schema}.silver_{entity}"

        business_columns = [
            field.name
            for field in spark.table(silver_table).schema.fields
            if field.name not in SYSTEM_COLUMNS
        ]

        rows = (
            spark.table(silver_table)
            .filter("_is_deleted = false")
            .orderBy(primary_key)
            .select(*business_columns)
            .limit(2)
            .collect()
        )

        if len(rows) < 2:
            raise ValueError(
                f"At least two active records are required in {silver_table}"
            )

        update_row, delete_row = rows

        records.append({
            "entity": entity,
            "operation": "UPDATE",
            "event_sequence": event_sequence,
            "event_timestamp": event_timestamp,
            "primary_key": str(update_row[primary_key]),
            "payload": build_updated_payload(update_row, primary_key),
        })
        event_sequence += 1

        records.append({
            "entity": entity,
            "operation": "DELETE",
            "event_sequence": event_sequence,
            "event_timestamp": event_timestamp,
            "primary_key": str(delete_row[primary_key]),
            "payload": None,
        })
        event_sequence += 1

    event_df = spark.createDataFrame(records, schema=EVENT_SCHEMA)

    write_mode = (
        "append"
        if spark.catalog.tableExists(events_table)
        else "ignore"
    )

    (
        event_df.write
        .format("delta")
        .mode(write_mode)
        .saveAsTable(events_table)
    )

    print(f"Added simulated CDC events to: {events_table}")
    print(f"New events in this batch: {len(records)}")
    print(f"Sequence range: {event_sequence - len(records)} to {event_sequence - 1}")
