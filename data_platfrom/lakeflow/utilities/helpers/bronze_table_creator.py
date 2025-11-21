from pyspark import pipelines as dp
from pyspark.sql import functions as F
from typing import Dict, Optional, Callable


def create_bronze_table(
    spark,
    table_name: str,
    source_path: str,
    comment: Optional[str] = None,
    additional_table_properties: Optional[Dict[str, str]] = None,
) -> Callable:
    """Creates a bronze streaming table with standardized audit columns and configurations.

    Args:
        spark: Active Spark session used for streaming reads. - Required 
        table_name: Name of the bronze streaming table to create. - Required
        source_path: Relative path to the source Delta table within the landing container. - Required
            Full path will be constructed as: abfss://landing@/{source_path}

        comment: Human-readable description of the table's contents and purpose.
        additional_table_properties: Custom Delta table properties. Defaults to None.

    Returns:
        A decorated function that DLT will execute to create the streaming table.

    Note:
        The following table properties are applied by default:
        
        - quality: "bronze" - Identifies this as a bronze layer table
        - pipelines.reset.allowed: "true" - Allows full refresh of the pipeline
        - delta.autoOptimize.optimizeWrite: "true" - Optimizes file sizes during writes
        - delta.autoOptimize.autoCompact: "true" - Automatically compacts small files
        - delta.columnMapping.mode: "name" - Enables column name mapping for schema evolution
    """
        
    # Merge default table properties with user-provided overrides
    # User properties take precedence over defaults via dictionary unpacking
    table_properties = {
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
        **(additional_table_properties or {})
    }
    
    # Generate descriptive comment if not explicitly provided
    table_comment = comment or f"Bronze layer: Raw CDF changes from source {table_name}"
    
    @dp.table(
        name=table_name,
        comment=table_comment,
        table_properties=table_properties,
        cluster_by_auto=True,  # Enable predictive optimization for query performance
    )
    def bronze_table():
        """
        Inner function that defines the streaming table logic.
        This function is executed by DLT to create and maintain the bronze table.
        """
        # Construct full ABFSS path to the source Delta table
        # TODO: Consider parameterizing storage account name for multi-environment support
        full_path = f"abfss://landing@stsegdbxpocnewdev.dfs.core.windows.net/{source_path}"
        
        return (
            spark.readStream
            .format("delta")
            .option("mergeSchema", "true")
            .load(full_path)
            .select(
                F.col("_metadata.file_path").alias("_raw_file_path"),
                F.col("_metadata.file_modification_time").alias("_raw_file_modification_time"),
                "*"  
            )
        )
    
    return bronze_table