"""
Report on Operational Subsidiaries (ROOS) Pipeline
--------------------------------------------------
Consolidates financial statements and operational metrics across 
multiple domestic and international subsidiaries using broadcast optimization.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def process_roos_pipeline(spark: SparkSession, accounts_path: str, subsidiary_master_df):
    accounts_df = spark.read.parquet(accounts_path)

    # Step 1: Broadcast Join to enrich account data with Subsidiary Metadata
    enriched_df = accounts_df.join(
        F.broadcast(subsidiary_master_df),
        on="subsidiary_id",
        how="inner"
    )

    # Step 2: Aggregate Financial Statements by Subsidiary Entity
    roos_summary_df = (
        enriched_df
        .groupBy("as_of_date", "subsidiary_id", "subsidiary_name", "entity_type")
        .agg(
            F.sum("total_assets").alias("consolidated_assets"),
            F.sum("total_liabilities").alias("consolidated_liabilities"),
            F.sum("net_interest_income").alias("total_nii")
        )
        .withColumn(
            "net_position",
            F.col("consolidated_assets") - F.col("consolidated_liabilities")
        )
    )
    return roos_summary_df
