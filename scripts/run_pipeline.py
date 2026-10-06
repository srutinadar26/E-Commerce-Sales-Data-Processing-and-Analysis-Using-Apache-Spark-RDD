"""
scripts/run_pipeline.py
-----------------------
Main entry point for the E-Commerce Spark RDD pipeline.

Execution order
---------------
  1. Generate raw dataset (if it doesn't exist)
  2. Start SparkSession
  3. Load + clean data (RDD)
  4. Run all analytics (RDD)
  5. Save cleaned data CSV
  6. Generate charts (Pandas / Matplotlib)
  7. Stop SparkSession

Run
---
    python scripts/run_pipeline.py
"""

import os
import sys
import time

# -- Project root on sys.path -------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger, load_config
from src.spark_session import get_spark_session, stop_spark_session
from src.data_cleaner import load_and_clean_rdd, save_cleaned_data
from src.rdd_analytics import run_all_analytics
from src.visualizer import generate_all_charts
from src.api_ingestion import run_api_ingestion

logger = get_logger(__name__)


def main():
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("  E-Commerce Sales Data Pipeline  –  START")
    logger.info("=" * 60)

    # -- Load configuration ----------------------------------------------------
    cfg = load_config()

    raw_path = os.path.join(PROJECT_ROOT, cfg["paths"]["raw_data"])
    output_dir = os.path.join(PROJECT_ROOT, cfg["paths"]["output_dir"])
    cleaned_csv = os.path.join(output_dir, cfg["output_files"]["cleaned_data"])

    # -- Step 1a: Try to fetch live data from DummyJSON API -------------------
    api_output = os.path.join(PROJECT_ROOT, "data", "raw", "ecommerce_sales_api.csv")
    if not os.path.exists(api_output):
        logger.info("Attempting API ingestion from DummyJSON …")
        try:
            run_api_ingestion(api_output, num_records=2000)
            logger.info("API ingestion succeeded: %s", api_output)
        except Exception as api_err:
            logger.warning("API ingestion failed (%s). Skipping API layer.", api_err)
    else:
        logger.info("API dataset already exists: %s", api_output)

    # -- Step 1b: Generate synthetic CSV if absent ----------------------------
    if not os.path.exists(raw_path):
        logger.info("Raw dataset not found. Generating …")
        from scripts.generate_data import main as gen_main
        gen_main()
    else:
        logger.info("Raw dataset found: %s", raw_path)

    # -- Step 2: Start SparkSession --------------------------------------------
    logger.info("Starting SparkSession …")
    spark = get_spark_session(
        app_name=cfg["spark"]["app_name"],
        master=cfg["spark"]["master"],
        driver_memory=cfg["spark"]["driver_memory"],
        log_level=cfg["spark"]["log_level"],
    )
    sc = spark.sparkContext
    master_url = getattr(sc, "master", "mock[local]")  # MockSparkContext has no .master
    logger.info("SparkSession started. Master: %s", master_url)

    try:
        # -- Step 3: Load + clean ----------------------------------------------
        logger.info("Loading and cleaning data …")
        cleaned_rdd = load_and_clean_rdd(sc, raw_path)

        # -- Step 4: Analytics -------------------------------------------------
        logger.info("Running analytics …")
        results = run_all_analytics(cleaned_rdd, output_dir)

        # -- Step 5: Save cleaned CSV ------------------------------------------
        logger.info("Saving cleaned data …")
        save_cleaned_data(cleaned_rdd, cleaned_csv)

        # -- Step 6: Charts ----------------------------------------------------
        logger.info("Generating charts …")
        generate_all_charts(output_dir)

        # -- Summary printout --------------------------------------------------
        summary = results.get("summary", {})
        elapsed = round(time.time() - start_time, 1)

        print("\n" + "=" * 60)
        print("  PIPELINE COMPLETE")
        print("=" * 60)
        print(f"  Total Transactions : {summary.get('total_transactions', 'N/A'):,}")
        print(f"  Total Revenue      : ${summary.get('total_revenue', 0):,.2f}")
        print(f"  Avg Order Value    : ${summary.get('avg_order_value', 0):,.2f}")
        print(f"  Unique Products    : {summary.get('unique_products', 'N/A')}")
        print(f"  Unique Customers   : {summary.get('unique_customers', 'N/A')}")
        print(f"  Elapsed time       : {elapsed}s")
        print(f"  Output directory   : {output_dir}")
        print("=" * 60)

    finally:
        # -- Step 7: Stop Spark ------------------------------------------------
        stop_spark_session(spark)
        logger.info("SparkSession stopped.")

    logger.info("Pipeline finished in %.1f seconds.", time.time() - start_time)


if __name__ == "__main__":
    main()
