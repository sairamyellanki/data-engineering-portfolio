"""
Financial Soundness Indicators (FSI) Pipeline
---------------------------------------------
Computes core FSI capital adequacy ratios, risk-weighted assets (RWA),
and asset quality indicators for regulatory health submissions.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def process_fsi_pipeline(spark: SparkSession, risk_data_path: str):
    df = spark.read.parquet(risk_data_path)

    # Step 1: Compute Risk-Weighted Assets (RWA) based on risk weights
    rwa_df = df.withColumn(
        "calculated_rwa",
        F.col("asset_book_value") * (F.col("risk_weight_percentage") / 100.0)
    )

    # Step 2: Aggregate Capital Allocation and RWA at the Bank Level
    bank_aggregate_df = (
        rwa_df
        .groupBy("as_of_date", "reporting_entity")
        .agg(
            F.sum("calculated_rwa").alias("total_risk_weighted_assets"),
            F.sum("tier1_capital_amount").alias("total_tier1_capital"),
            F.sum("non_performing_asset_amt").alias("total_npa")
        )
    )

    # Step 3: Calculate Final FSI Capital Adequacy Ratios
    fsi_final_df = bank_aggregate_df.withColumn(
        "fsi_tier1_capital_adequacy_ratio",
        F.when(
            F.col("total_risk_weighted_assets") > 0,
            (F.col("total_tier1_capital") / F.col("total_risk_weighted_assets")) * 100.0
        ).otherwise(0.0)
    ).withColumn(
        "gross_npa_ratio",
        F.when(
            F.col("total_risk_weighted_assets") > 0,
            (F.col("total_npa") / F.col("total_risk_weighted_assets")) * 100.0
        ).otherwise(0.0)
    )
    
    return fsi_final_df
