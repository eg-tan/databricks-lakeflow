import dlt
from pyspark.sql import functions as F
from typing import Dict, Optional, Callable


def create_bronze_table(
    spark,
    table_name: str,
    source_path: str,
    comment: Optional[str] = None,
    additional_table_properties: Optional[Dict[str, str]] = None,
) -> Callable:
    """
    Creates a bronze DLT streaming table with standardized audit columns and configurations.
    
    This function generates a DLT table decorator that:
    - Creates bronze streaming table 
    - Adds file-level audit columns for data lineage
    - Enables automatic clustering for query performance
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session used for streaming reads.
        
    table_name : str
        Name of the bronze streaming table to create.
        
    source_path : str
        Relative path to the source Delta table within the landing container.
        Full path will be constructed as: abfss://landing@<storage>/{source_path}
        
    comment : str, optional
        Human-readable description of the table's contents and purpose.
        If not provided, a default comment will be generated.
        
    additional_table_properties : dict, optional
        Custom Delta table properties to merge with standard properties.
        These will override defaults if keys conflict.
        Example: {"delta.enableChangeDataFeed": "true", "delta.deletedFileRetentionDuration": "interval 30 days"}
        
    Returns
    -------
    Callable
        A decorated function that DLT will execute to create the streaming table.
        
    Standard Table Properties
    -------------------------
    The following properties are applied by default:
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
    
    @dlt.table(
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
            # Add file-level lineage metadata from Spark's _metadata pseudo-column
            .withColumn("_raw_file_path", F.col("_metadata.file_path"))
            .withColumn("_raw_file_modification_time", F.col("_metadata.file_modification_time"))
        )
    
    return bronze_table