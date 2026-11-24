
import argparse

from databricks.sdk.runtime import spark

from enterprise_data_pipeline.gold.transformations import transform_daily_sales
from enterprise_data_pipeline.ingestion.autoloader import ingest_all
from enterprise_data_pipeline.silver.transformations import transform_all


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

    print("Starting Silver transformations...")
    transform_all(spark=spark, catalog=catalog, schema=schema)
    print("Silver transformations completed successfully.")

    print("Starting Gold transformations...")
    transform_daily_sales(spark, catalog, schema)
    print("Gold transformations completed successfully.")


if __name__ == "__main__":
    main()
