from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict, Optional, Literal

def create_silver_scd_table(
    entity_name: str,
    business_keys: List[str],
    scd_type: Literal[1, 2] = 2,
    source_view: Optional[str] = None,
    custom_table_properties: Optional[Dict[str, str]] = None,
    custom_except_columns: Optional[List[str]] = None,
    sequence_by_column: str = "_raw_commit_timestamp",
    cluster_by_keys: bool = False,
    cluster_by_auto: bool = True,
    track_history_columns: Optional[List[str]] = None,
    ignore_null_updates: bool = False,
) -> None:
    """
    Creates a complete SCD pipeline with configurable SCD type (1 or 2).
    
    This function sets up a streaming table with Change Data Capture (CDC) that
    tracks changes using either SCD Type 1 (updates in place) or SCD Type 2 (historical tracking).
    
    Args:
        entity_name: Base name for the entity - Required
        business_keys: List of column names that form the primary key - Required
        scd_type: SCD type - either 1 (update in place) or 2 (track history). Defaults to 2. - Required

        source_view: Name of the source view. Defaults to f"vw_{entity_name}_cleaned".
        custom_table_properties: Additional Delta table properties to merge with defaults.
        custom_except_columns: Column names to exclude from CDC tracking.
        sequence_by_column: Column name used for CDC sequencing. Defaults to "_raw_commit_timestamp".
        cluster_by_keys: If True, manually cluster by business keys. Defaults to False.
                        Cannot be True if cluster_by_auto is True.
        cluster_by_auto: If True, enable auto-clustering (recommended). Defaults to True.
                        Cannot be True if cluster_by_keys is True.
        track_history_columns: Specific columns to track for SCD2 history. Only applicable for SCD Type 2.
        ignore_null_updates: If True, null values won't trigger updates. Defaults to False.
    
    Returns:
        None

    """
    
    # ========================================
    # INPUT VALIDATION
    # ========================================
    
    # Validate entity_name
    if not entity_name or not entity_name.strip():
        raise ValueError("entity_name cannot be empty or whitespace")
    
    entity_name = entity_name.strip().lower()
    
    # Validate business_keys
    if not business_keys:
        raise ValueError("business_keys cannot be empty - at least one key is required")
    
    # Validate scd_type
    if scd_type not in (1, 2):
        raise ValueError(f"scd_type must be either 1 or 2, got: {scd_type}")
    
    # Validate clustering options (mutually exclusive)
    if cluster_by_keys and cluster_by_auto:
        raise ValueError(
            "Conflicting clustering options: cluster_by_keys and cluster_by_auto "
            "cannot both be True. Choose one clustering strategy"
        )
    
    # ========================================
    # CONFIGURATION SETUP
    # ========================================
    
    source_view = source_view or f"vw_{entity_name}_cleaned"
    silver_table = f"stbl_{entity_name}"
    
    # Default table properties with enhanced optimization
    table_properties = {
        "quality": "silver",
        "pipelines.reset.allowed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipeline.scd.type": str(scd_type),
        "pipeline.scd.keys": ", ".join(business_keys),
    }
    
    # Merge custom properties (custom properties override defaults)
    if custom_table_properties:
        table_properties.update(custom_table_properties)
    
    # Build streaming table configuration
    scd_type_description = "history tracking" if scd_type == 2 else "update in place"
    streaming_table_kwargs = {
        "name": silver_table,
        "comment": (
            f"Silver layer {entity_name} table with SCD Type {scd_type} ({scd_type_description}). "
            f"Primary keys: {', '.join(business_keys)}"
        ),
        "table_properties": table_properties,
    }
    
    # Add clustering configuration (mutually exclusive)
    if cluster_by_auto:
        streaming_table_kwargs["cluster_by_auto"] = True
    elif cluster_by_keys:
        streaming_table_kwargs["cluster_by"] = business_keys
    
    # ========================================
    # TABLE CREATION 
    # ========================================

    dp.create_streaming_table(**streaming_table_kwargs)
    
    # ========================================
    # CDC FLOW CREATION 
    # ========================================

    # Build CDC flow configuration
    cdc_kwargs = {
        "target": silver_table,
        "source": source_view,
        "keys": business_keys,
        "sequence_by": F.col(sequence_by_column),
        "apply_as_deletes": "_raw_change_type = 'delete'",
        "stored_as_scd_type": str(scd_type),
        "ignore_null_updates": ignore_null_updates,
    }
    
    # Add optional CDC parameters
    if custom_except_columns:
        cdc_kwargs["except_column_list"] = custom_except_columns
    
    # Only add track_history_column_list for SCD Type 2
    if scd_type == 2 and track_history_columns:
        cdc_kwargs["track_history_column_list"] = track_history_columns

    # Create CDC Flow
    dp.create_auto_cdc_flow(**cdc_kwargs)


