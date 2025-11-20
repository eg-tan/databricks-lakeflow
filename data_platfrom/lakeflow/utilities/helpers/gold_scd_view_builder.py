from pyspark import pipelines as dp
from pyspark.sql import DataFrame, functions as F
from typing import List, Optional

DEFAULT_START_TIMESTAMP = "1900-01-01 00:00:00"
DEFAULT_END_TIMESTAMP = "9999-12-31 23:59:59"

DEFAULT_START_COL = "__START_AT"
DEFAULT_END_COL = "__END_AT"

def create_gold_scd2_view(
    entity_name: str,
    silver_scd2_table: str,
    dim_key_col: str,
    business_cols: List[str],
    business_key_col: str = "business_key",
    comment: Optional[str] = None
) -> None:
    """
    Create a Delta Live Tables view with SCD2 semantics for Gold layer.
    
    Args:
        entity_name: Entity name for the view (e.g., "customers")
        silver_scd2_table: Fully qualified Silver table name
        dim_key_col: Name for the surrogate key column
        business_cols: Business columns to include (in desired order)
        business_key_col: Name of the business key column
        comment: Optional view comment (auto-generated if None)
    
    Features:
        - Generates surrogate key via xxhash64
        - Normalizes earliest _row_valid_from to '1900-01-01 00:00:00'
        - Normalizes null _row_valid_to to '9999-12-31 23:59:59'
        - Derives is_current flag (_row_valid_to = '9999-12-31 23:59:59')
        - Derives is_deleted flag (latest closed version with no current)
    
    Conventions:
        - Source must have __START_AT and __END_AT columns
        - _row_valid_from normalized to '1900-01-01 00:00:00' for earliest records
        - _row_valid_to normalized to '9999-12-31 23:59:59' for current records
    """
    
    mat_view_name = f"dim_{entity_name}"
    mat_view_comment = comment or f"{entity_name.title()} dimension with SCD2 tracking"

    @dp.materialized_view(name=view_name, comment=view_comment)
    def dim_entity_scd2() -> DataFrame:
        
        # Read source table
        df = dlt.read(silver_scd2_table)
        
        # Validate required columns exist
        required_cols = [DEFAULT_START_COL, DEFAULT_END_COL, business_key_col]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Silver table '{silver_scd2_table}' missing required columns: {missing_cols}"
            )
        
        # Create base transformation with surrogate key
        base = df.select(
            F.xxhash64(business_key_col).alias(dim_key_col),
            F.col(business_key_col),
            *[F.col(c) for c in business_cols],
            F.col(DEFAULT_START_COL).alias("_row_valid_from"),
            F.col(DEFAULT_END_COL).alias("_row_valid_to")
        )
        
        # Compute aggregates per business key (single pass with window functions)
        enriched = base.withColumn(
            "min_row_valid_from",
            F.min("_row_valid_from").over(F.Window.partitionBy(business_key_col))
        ).withColumn(
            "latest_closed_start",
            F.max(
                F.when(F.col("_row_valid_to").isNotNull(), F.col("_row_valid_from"))
            ).over(F.Window.partitionBy(business_key_col))
        ).withColumn(
            "has_current_flag",
            F.max(
                F.when(F.col("_row_valid_to").isNull(), F.lit(1)).otherwise(F.lit(0))
            ).over(F.Window.partitionBy(business_key_col))
        )
        
        # Build final result with derived columns
        result = enriched.select(
            F.col(dim_key_col),
            *[F.col(c) for c in business_cols],
            
            # Normalize earliest record to default start timestamp
            F.when(
                F.col("_row_valid_from") == F.col("min__row_valid_from"),
                F.expr(f"timestamp'{DEFAULT_START_TIMESTAMP}'")
            ).otherwise(F.col("_row_valid_from")).alias("_row_valid_from"),
            
            # Normalize null _row_valid_to to default end timestamp
            F.coalesce(
                F.col("_row_valid_to"),
                F.expr(f"timestamp'{DEFAULT_END_TIMESTAMP}'")
            ).alias("_row_valid_to"),
            
            # Current record: _row_valid_to equals default end timestamp
            (F.col("_row_valid_to").isNull()).alias("is_current"),
            
            # Deleted: latest closed version AND no current version exists
            (
                F.col("_row_valid_to").isNotNull()
                & (F.col("_row_valid_from") == F.col("latest_closed_start"))
                & (F.col("has_current_flag") == 0)
            ).alias("is_deleted")
        )
        
        return result
    
    return vw_dim_entity