
import argparse

from databricks.sdk.runtime import spark

from enterprise_data_pipeline.cdc.event_source import create_demo_cdc_events
from enterprise_data_pipeline.cdc.processor import (
    apply_cdc_events,
    get_silver_table_versions,
    prepare_silver_tables,
    read_silver_cdf,
)
from enterprise_data_pipeline.gold.transformations import transform_daily_sales
from enterprise_data_pipeline.ingestion.autoloader import ingest_all
from enterprise_data_pipeline.silver.transformations import bootstrap_silver_tables


def main():
    parser = argparse.ArgumentParser(description="Enterprise Data Pipeline")

    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)

    args = parser.parse_args()

    catalog = args.catalog
    schema = args.schema

    print(f"Starting pipeline for {catalog}.{schema}")

    print("Starting Bronze ingestion...")
    ingest_all(spark=spark, catalog=catalog, schema=schema)
    print("Bronze ingestion completed successfully.")

    print("Starting Silver bootstrap...")
    bootstrap_silver_tables(spark=spark, catalog=catalog, schema=schema)
    print("Silver bootstrap completed successfully.")

    print("Preparing Silver tables for CDC...")
    prepare_silver_tables(spark=spark, catalog=catalog, schema=schema)
    print("Silver CDC preparation completed.")

    print("Creating simulated CDC events...")
    create_demo_cdc_events(spark=spark, catalog=catalog, schema=schema)
    print("CDC event source ready.")

    print("Capturing Silver table versions before CDC...")
    starting_versions = get_silver_table_versions(
        spark, catalog, schema
    )

    print("Applying CDC events to Silver...")
    apply_cdc_events(spark=spark, catalog=catalog, schema=schema)
    print("CDC processing completed.")

    print("Reading changes using Delta CDF...")
    read_silver_cdf(
        spark=spark,
        catalog=catalog,
        schema=schema,
        starting_versions=starting_versions,
    )
    print("CDF validation completed.")

    print("Starting Gold transformations...")
    transform_daily_sales(spark, catalog, schema)
    print("Gold transformations completed successfully.")

    print("Enterprise Data Pipeline completed successfully.")


if __name__ == "__main__":
    main()
