
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_daily_sales(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> DataFrame:
    """Build daily sales metrics from Silver orders and order items."""

    orders_table = f"{catalog}.{schema}.silver_orders"
    items_table = f"{catalog}.{schema}.silver_order_items"

    orders = spark.table(orders_table)
    order_items = spark.table(items_table)

    # Calculate total value for each order.
    order_amounts = (
        order_items
        .groupBy("order_id")
        .agg(
            F.sum(
                F.col("quantity") * F.col("unit_price")
            ).alias("order_amount")
        )
    )

    # Join order details with calculated order amounts.
    order_sales = (
        orders
        .join(order_amounts, on="order_id", how="left")
        .withColumn(
            "order_amount",
            F.coalesce(F.col("order_amount"), F.lit(0).cast("decimal(18,2)")),
        )
    )

    # Aggregate daily business metrics.
    daily_sales = (
        order_sales
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct(
                F.when(F.col("status") == "DELIVERED", F.col("order_id"))
            ).alias("delivered_orders"),
            F.sum(
                F.when(
                    F.col("status") == "DELIVERED",
                    F.col("order_amount"),
                ).otherwise(F.lit(0))
            ).alias("total_revenue"),
            F.avg(
                F.when(
                    F.col("status") == "DELIVERED",
                    F.col("order_amount"),
                )
            ).alias("average_order_value"),
        )
        .orderBy("order_date")
    )

    return daily_sales


def write_gold(df: DataFrame, catalog: str, schema: str) -> None:
    """Write the Gold daily sales table as a Delta table."""

    table = f"{catalog}.{schema}.gold_daily_sales"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )

    print(f"Written {df.count()} records to {table}")


def transform_daily_sales(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> None:
    print("Building Gold daily sales table...")

    df = build_daily_sales(spark, catalog, schema)

    write_gold(df, catalog, schema)

    print("Gold daily sales transformation completed successfully.")
