"""
Interest Rate Sensitivity (IRS) Gap Analysis Pipeline
------------------------------------------------------
Categorizes balance-sheet assets/liabilities into repricing time buckets 
and computes weighted interest exposure for RBI risk reporting.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def process_irs_pipeline(spark: SparkSession, input_path: str):
    df = spark.read.parquet(input_path)

    # Step 1: Assign Repricing Time Buckets for IRS Gap Analysis
    bucketed_df = df.withColumn(
        "irs_gap_bucket",
        F.when(F.col("repricing_days") <= 90, "1-90 Days (High Sensitivity)")
         .when((F.col("repricing_days") > 90) & (F.col("repricing_days") <= 365), "91-365 Days (Medium Sensitivity)")
         .otherwise("1+ Year (Low Sensitivity)")
    )

    # Step 2: Calculate Weighted Interest Exposure
    exposed_df = bucketed_df.withColumn(
        "weighted_interest_exposure",
        F.col("principal_amount") * (F.col("interest_rate") / 100.0)
    )

    # Step 3: Aggregate by Gap Bucket for RBI Submission
    summary_df = (
        exposed_df
        .groupBy("as_of_date", "irs_gap_bucket")
        .agg(
            F.sum("principal_amount").alias("total_principal_exposure"),
            F.sum("weighted_interest_exposure").alias("total_weighted_exposure"),
            F.count("account_id").alias("account_count")
        )
    )
    return summary_df
