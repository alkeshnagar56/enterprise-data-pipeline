
import argparse

from databricks.sdk.runtime import spark
from enterprise_data_pipeline.ingestion.autoloader import ingest_orders


def main():
    parser = argparse.ArgumentParser(
        description="Enterprise Data Pipeline"
    )

    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)

    args = parser.parse_args()

    catalog = args.catalog
    schema = args.schema

    print(f"Starting pipeline for {catalog}.{schema}")

    spark.sql(f"USE CATALOG `{catalog}`")
    spark.sql(f"USE SCHEMA `{schema}`")

    ingest_orders(
        spark=spark,
        catalog=catalog,
        schema=schema
    )

    print("Enterprise Data Pipeline execution completed.")


if __name__ == "__main__":
    main()
