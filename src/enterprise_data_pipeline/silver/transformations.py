
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def clean_strings(df: DataFrame) -> DataFrame:
    """Trim string columns and convert empty strings to NULL."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            column = F.trim(F.col(field.name))
            df = df.withColumn(
                field.name,
                F.when(F.length(column) == 0, None).otherwise(column),
            )
    return df


def read_bronze(spark: SparkSession, catalog: str, schema: str, entity: str) -> DataFrame:
    table = f"{catalog}.{schema}.bronze_{entity}"
    return clean_strings(spark.table(table))


def write_silver(df: DataFrame, catalog: str, schema: str, entity: str) -> None:
    table = f"{catalog}.{schema}.silver_{entity}"

    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(table)

    print(f"Written {df.count()} records to {table}")


def transform_customers(spark: SparkSession, catalog: str, schema: str) -> None:
    df = read_bronze(spark, catalog, schema, "customers")

    df = (
        df.withColumn("signup_date", F.to_date("signup_date", "dd/MM/yyyy"))
        .dropDuplicates(["customer_id"])
        .filter(F.col("customer_id").isNotNull())
    )

    write_silver(df, catalog, schema, "customers")


def transform_orders(spark: SparkSession, catalog: str, schema: str) -> None:
    df = read_bronze(spark, catalog, schema, "orders")

    df = (
        df.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        .dropDuplicates(["order_id"])
        .filter(F.col("order_id").isNotNull())
    )

    write_silver(df, catalog, schema, "orders")


def transform_order_items(spark: SparkSession, catalog: str, schema: str) -> None:
    df = read_bronze(spark, catalog, schema, "order_items")

    df = (
        df.withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
        .dropDuplicates(["order_item_id"])
        .filter(F.col("order_item_id").isNotNull())
    )

    write_silver(df, catalog, schema, "order_items")


def transform_products(spark: SparkSession, catalog: str, schema: str) -> None:
    df = read_bronze(spark, catalog, schema, "products")

    df = (
        df.withColumn("price", F.col("price").cast("decimal(10,2)"))
        .dropDuplicates(["product_id"])
        .filter(F.col("product_id").isNotNull())
    )

    write_silver(df, catalog, schema, "products")


def transform_all(spark: SparkSession, catalog: str, schema: str) -> None:
    transformations = (
        ("customers", transform_customers),
        ("orders", transform_orders),
        ("order_items", transform_order_items),
        ("products", transform_products),
    )

    for entity, transform in transformations:
        print(f"Starting Silver transformation: {entity}")
        transform(spark, catalog, schema)
        print(f"Completed Silver transformation: {entity}")

    print("All Silver transformations completed successfully.")




def bootstrap_silver_tables(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> None:
    """Create Silver tables only when they do not already exist."""

    transformations = (
        ("customers", transform_customers),
        ("orders", transform_orders),
        ("order_items", transform_order_items),
        ("products", transform_products),
    )

    for entity, transform in transformations:
        table = f"{catalog}.{schema}.silver_{entity}"

        if spark.catalog.tableExists(table):
            print(f"Silver table already exists. Skipping bootstrap: {table}")
            continue

        print(f"Silver table missing. Bootstrapping: {table}")
        transform(spark, catalog, schema)

    print("Silver bootstrap completed.")
