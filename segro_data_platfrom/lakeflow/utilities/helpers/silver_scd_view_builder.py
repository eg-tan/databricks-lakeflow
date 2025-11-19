# from pyspark import pipelines as dp
# from pyspark.sql import DataFrame
# from pyspark.sql import functions as F
# from typing import List


# def create_silver_scd_view(
#     silver_table: str,
#     silver_mat_view: str,
#     business_keys: List[str],
#     comment: str = None
# ) -> callable:
#     """
#     Creates a  function for generating  SCD2 views.
    
#     Args:
#         silver_table: Name of the source silver table with SCD2 tracking
#         silver_mat_view: Name of the materialized view to create
#         business_keys: List of column names that form the business key
        
#     Returns:
#         A decorated DLT table function
#     """
    
#     dp.materialized_view(
#         name=silver_mat_view,
#         comment= comment or 'SCD2 view with standardized effective dates and status flags'
#     )
#     def create_mat_view() -> DataFrame:
#         """
#         Creates a Gold-ready SCD2 view with:
#         - Hash-based surrogate key
#         - Standardized effective dates (1900-01-01 for first, 9999-12-31 for current)
#         - is_current flag
#         - is_deleted flag
#         """
        
#         # Read the silver table
#         df_silver = dlt.read(silver_table)
        
#         # Build business key expression for hashing
#         if len(business_keys) == 1:
#             hash_expr = F.col(business_keys[0])
#         else:
#             # For composite keys, concatenate with delimiter
#             hash_expr = F.concat_ws("||", *[F.col(k) for k in business_keys])
        
#         # Create composite business key column for grouping
#         if len(business_keys) == 1:
#             business_key_col = F.col(business_keys[0]).alias("business_key")
#         else:
#             business_key_col = F.concat_ws("||", *[F.col(k) for k in business_keys]).alias("business_key")
        
#         # Anchored CTE: Base data with hash key and business key
#         anchored = (
#             df_silver
#             .withColumn("business_key", business_key_col)
#             .withColumn("hash_key", F.xxhash64(hash_expr))
#             .withColumn("effective_from", F.col("__START_AT"))
#             .withColumn("effective_thru", F.col("__END_AT"))
#         )
        
#         # Per-key aggregations: minimum effective_from per business key
#         per_key_min = (
#             anchored
#             .groupBy("business_key")
#             .agg(F.min("effective_from").alias("min_effective_from"))
#         )
        
#         # Per-key aggregations: latest closed record's start date
#         per_key_closed = (
#             anchored
#             .filter(F.col("effective_thru").isNotNull())
#             .groupBy("business_key")
#             .agg(F.max("effective_from").alias("latest_closed_start"))
#         )
        
#         # Per-key aggregations: check if business key has any current record
#         per_key_current = (
#             anchored
#             .groupBy("business_key")
#             .agg(
#                 F.max(
#                     F.when(F.col("effective_thru").isNull(), F.lit(1))
#                     .otherwise(F.lit(0))
#                 ).alias("has_current")
#             )
#         )
        
#         # Get all original columns except internal ones
#         exclude_cols = {"hash_key", "business_key", "effective_from", 
#                        "effective_thru", "__START_AT", "__END_AT"}
#         original_cols = [c for c in df_silver.columns if c not in exclude_cols]
        
#         # Final join and transformations
#         result = (
#             anchored.alias("a")
#             .join(per_key_min.alias("pkm"), "business_key", "inner")
#             .join(per_key_closed.alias("pkc"), "business_key", "left")
#             .join(per_key_current.alias("pkcur"), "business_key", "left")
#             .select(
#                 F.col("a.hash_key"),
#                 *[F.col(f"a.{bk}") for bk in business_keys],  # Original business key columns
                
#                 # All original columns (excluding internal SCD columns)
#                 *[F.col(f"a.{c}") for c in original_cols],
                
#                 # Standardized effective_from (epoch for first record)
#                 F.when(
#                     F.col("a.effective_from") == F.col("pkm.min_effective_from"),
#                     F.lit("1900-01-01 00:00:00").cast("timestamp")
#                 ).otherwise(F.col("a.effective_from")).alias("effective_from"),
                
#                 # Standardized effective_thru (9999-12-31 for current records)
#                 F.when(
#                     F.col("a.effective_thru").isNull(),
#                     F.lit("9999-12-31 23:59:59").cast("timestamp")
#                 ).otherwise(F.col("a.effective_thru")).alias("effective_thru"),
                
#                 # is_current flag: True if effective_thru is NULL
#                 F.col("a.effective_thru").isNull().alias("is_current"),
                
#                 # is_deleted flag: Latest closed record with no current record
#                 F.when(
#                     F.col("a.effective_thru").isNotNull() &
#                     (F.col("a.effective_from") == F.col("pkc.latest_closed_start")) &
#                     (F.coalesce(F.col("pkcur.has_current"), F.lit(0)) == 0),
#                     F.lit(True)
#                 ).otherwise(F.lit(False)).alias("is_deleted")
#             )
#         )
        
#         return result
    
#     # Return the decorated function
#     return create_gold_view