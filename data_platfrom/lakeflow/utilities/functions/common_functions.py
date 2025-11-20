from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
from typing import List, Union
from pyspark.sql import Column

#------------------------------------------------------------------------------------------------
# get_business_key
#------------------------------------------------------------------------------------------------

def get_business_key(
    columns: Union[List[str], List[Column]],
    delimiter: str = "|",
    alias: str = "business_key"
) -> Column:
    """
    Creates a deterministic business key by concatenating multiple columns using a delimiter.

    Args:
        columns (List[str] | List[Column]):
            Columns to include in the business key.
        delimiter (str, optional):
            Delimiter for joining column values. Defaults to "|".
        alias (str, optional):
            Output column alias. Defaults to "business_key".

    Returns:
        Column: A column representing the business key.
    """
    if not columns:
        raise ValueError("`columns` cannot be an empty list.")

    string_cols: List[Column] = []
    for col in columns:
        if isinstance(col, str):
            string_cols.append(F.col(col).cast(StringType()))
        elif isinstance(col, Column):
            string_cols.append(col.cast(StringType()))
        else:
            raise TypeError(f"Invalid column type: {type(col)}. Must be str or Column.")

    return F.concat_ws(delimiter, *string_cols).alias(alias)

#------------------------------------------------------------------------------------------------
# get_silver_metadata_columns
#------------------------------------------------------------------------------------------------

def get_silver_metadata_columns(include_file_metadata: bool = False) -> List[Column]:
    """
    Returns metadata columns for inline use in select statements in Silver layer.
    
    This function generates a list of PySpark Column objects representing metadata
    fields that track data lineage and timing information as data moves from Bronze
    to Silver layer in a medallion architecture. 
    Args:
        include_file_metadata (bool, optional): If True, includes source file path 
            and modification time in the metadata columns. Defaults to False.
    Returns:
        List[Column]: A list of PySpark Column objects containing:
            - _raw_change_type: Type of change operation (insert, update, delete)
            - _raw_commit_timestamp: Timestamp when the change was committed
            - _raw_commit_version: Version number of the commit
            - _bronze_timestamp: Timestamp when data entered Bronze layer
            - _silver_timestamp: Current timestamp when data enters Silver layer
            - _raw_file_path: (Optional) Source file path
            - _raw_file_timestamp: (Optional) Source file modification time

    """
    # Core metadata columns
    metadata_cols = [
        F.col("_raw_change_type"),
        F.col("_raw_commit_timestamp"),
        F.col("_raw_commit_version"),
        F.current_timestamp().alias("_silver_timestamp")
    ]
    
    # Optional file metadata columns
    if include_file_metadata:
        file_metadata_cols = [
            F.col("_metadata.file_path").alias("_raw_file_path"),
            F.col("_metadata.file_modification_time").alias("_raw_file_timestamp")
        ]
        metadata_cols.extend(file_metadata_cols)
    
    return metadata_cols


