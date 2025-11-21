# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Change Data Feed (CDF) Ingestion Notebook
# MAGIC
# MAGIC ### Overview
# MAGIC This notebook ingests incremental data from Delta tables using Change Data Feed functionality.
# MAGIC
# MAGIC ### Input Parameters
# MAGIC
# MAGIC | Parameter | Type | Required | Description |
# MAGIC |-----------|------|----------|-------------|
# MAGIC | `SOURCE_SYSTEM` | String | Yes | Source system identifier for data lineage tracking |
# MAGIC | `SOURCE_TABLE_PATH` | String | Yes | Absolute path to source Delta table (CDF-enabled) |
# MAGIC | `TARGET_TABLE_PATH` | String | Yes | Full path to target Delta table where changes will be appended |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Processing Logic
# MAGIC
# MAGIC - Reads incremental changes (inserts, updates, deletes) from CDF stream
# MAGIC - Preserves native CDF metadata columns with renamed prefixes to avoid conflicts:
# MAGIC   - `_change_type` → `_raw_change_type`: Operation type (insert, update_preimage, update_postimage, delete)
# MAGIC   - `_commit_version` → `_raw_commit_version`: Delta Lake transaction version number (monotonically increasing)
# MAGIC   - `_commit_timestamp` → `_raw_commit_timestamp`: Exact timestamp when change was committed at source
# MAGIC - Adds additional audit columns for data lineage and troubleshooting:
# MAGIC   - `_raw_file_path`: Source file location from Spark metadata
# MAGIC   - `_raw_file_modification_time`: File last modified timestamp
# MAGIC   - `_source_system`: Source system identifier (from input parameter)
# MAGIC   - `_ingestion_timestamp`: Pipeline processing timestamp (current time)
# MAGIC - Processes only new changes since last successful checkpoint
# MAGIC - Partitioned by `_processing_date`  and `_processing_timestamp` 
# MAGIC ---
# MAGIC
# MAGIC ### Output Schema
# MAGIC
# MAGIC The target table will contain all source columns plus the following audit/metadata columns:
# MAGIC
# MAGIC | Column | Type | Source | Description |
# MAGIC |--------|------|--------|-------------|
# MAGIC | `_raw_change_type` | String | CDF | Type of change operation |
# MAGIC | `_raw_commit_version` | Long | CDF | Version number of the change |
# MAGIC | `_raw_commit_timestamp` | Timestamp | CDF | When the change occurred |
# MAGIC | `_source_system` | String | Parameter | Source system identifier |
# MAGIC | `_processing_date` | Date | Pipeline | Date when the record was ingested |
# MAGIC | `_processing_timestamp` | Timestamp | Pipeline | Timestamp when the record was ingested |
# MAGIC ---
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC 1. Source must be a valid Delta table with enabled CDF
# MAGIC ```sql
# MAGIC      ALTER TABLE <source_table> 
# MAGIC      SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

dbutils.widgets.text(
    "SOURCE_SYSTEM",
    "---",
    "Source system"
)

# dbutils.widgets.text(
#     "TABLE_NAME",
#     "brz_region",
#     "Table name"
# )

dbutils.widgets.text(
    "SOURCE_TABLE_PATH",
    "---",
    "Source Delta table path"
)

dbutils.widgets.text(
    "TARGET_TABLE_PATH",
    "---",
    "Target Delta table path"
)

SOURCE_SYSTEM = dbutils.widgets.get("SOURCE_SYSTEM").strip()
# TABLE_NAME = dbutils.widgets.get("TABLE_NAME").strip()
SOURCE_TABLE_PATH = dbutils.widgets.get("SOURCE_TABLE_PATH").strip()
TARGET_TABLE_PATH = dbutils.widgets.get("TARGET_TABLE_PATH").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports, Constants, Logging
# MAGIC

# COMMAND ----------

import logging
import sys
from datetime import datetime
from typing import List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp, lit, when
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import LongType
from pyspark.sql.utils import AnalysisException

# Constants
SOURCE_CDF_CHANGE_TYPE_COLUMN = "_change_type"
SOURCE_CDF_COMMIT_VERSION_COLUMN = "_commit_version"
SOURCE_CDF_COMMIT_TIMESTAMP_COLUMN = "_commit_timestamp"

TARGET_CDF_CHANGE_TYPE_COLUMN = f"_raw{SOURCE_CDF_CHANGE_TYPE_COLUMN}"
TARGET_CDF_COMMIT_VERSION_COLUMN = f"_raw{SOURCE_CDF_COMMIT_VERSION_COLUMN}"
TARGET_CDF_COMMIT_TIMESTAMP_COLUMN = f"_raw{SOURCE_CDF_COMMIT_TIMESTAMP_COLUMN}"


PROCESSING_TIMESTAMP_COLUMN = "_processing_timestamp"
PROCESSING_DATE_COLUMN = "_processing_date"
SOURCE_SYSTEM_COLUMN = "_source_system"

DEFAULT_CHANGE_TYPE = "insert"

# Logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(funcName)s: %(message)s",
    force=True 
)
logger = logging.getLogger(__name__)

def _banner(text: str):
    logger.info("═" * 80)
    logger.info(text)
    logger.info("═" * 80)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Table utilities
# MAGIC

# COMMAND ----------

def is_table_exist(table_path: str) -> bool:
    """
    Check if a Delta table physically exists at the given ABFS path.
    Args:
        table_path: The ABFS path to the Delta table
    Returns:
        bool: True if a valid Delta table exists, False otherwise
    """ 
    log_path = table_path.rstrip("/") + "/_delta_log"
    try:
        return any(f.name.endswith(".json") for f in dbutils.fs.ls(log_path))
    except Exception:
        return False
            
    except Exception:
        # This is EXPECTED - table simply doesn't exist
        logger.debug(f"No Delta table found at {table_path}") 
        return False
      

def get_last_available_version(table_path: str) -> Optional[int]:
    """
    Get the maximum available version from a Delta table.
    Args:     
        table_path: Path to Delta table   
    Returns:
        Maximum version number or None if unable to retrieve
    """
    try:
        dt = DeltaTable.forPath(spark, table_path)
        return int(dt.history(1).select("version").first()["version"])
    except Exception as e:
        logger.warning(f"Could not read max version for {table_path}: {e}")
        return None
    
# def _non_empty(df: DataFrame) -> bool:
#     """
#     Check if a DataFrame contains at least one row without triggering a full count.
#     Args:
#         df: The DataFrame to check   
#     Returns:
#         True if the DataFrame contains at least one row, False if empty
#         """
#     return not df.rdd.isEmpty()

# COMMAND ----------

# MAGIC %md
# MAGIC #### CDF status, state and checkpoint management. 

# COMMAND ----------

def is_cdf_enabled(source_table_path: str) -> bool:
    """
    Check if Change Data Feed is enabled on the source table.
    Args:
        source_table_path: Path to source Delta table    
    Returns:
        True if CDF is enabled, False otherwise
    """
    try:
        delta_table = DeltaTable.forPath(spark, source_table_path)
        table_details = delta_table.detail().collect()[0]
        properties = table_details.asDict().get('properties', {})
        cdf_enabled = properties.get('delta.enableChangeDataFeed', 'false').lower() == 'true'
       
        return cdf_enabled
        
    except Exception as e:
        logger.error(f"Error checking CDF status: {e}")
        raise


def get_first_cdf_enabled_version(source_table_path: str) -> Optional[int]:
    """
    Find the first version where CDF was enabled.
    This version represents the boundary - data before this should be read as snapshot.
    Args:  
        source_table_path: Path to source Delta table  
    Returns:
        Version number where CDF was enabled, or None if not found
    """
    try:
        delta_table = DeltaTable.forPath(spark, source_table_path)
        history = delta_table.history()
        
        # Find the version where CDF was enabled
        cdf_enabled_row = (
            history
            .filter(
                (col("operation") == "SET TBLPROPERTIES") &
                (col("operationParameters.properties").contains("delta.enableChangeDataFeed"))
            )
            .orderBy("version")
            .select("version", "timestamp")
            .first()
        )
        
        if cdf_enabled_row:
            version = cdf_enabled_row["version"]
            timestamp = cdf_enabled_row["timestamp"]
            logger.info(f"First CDF-enabled version: {version} at {timestamp}")
            return version
        else:
            logger.warning("Could not find CDF enablement in table history")
            return None
            
    except Exception as e:
        logger.error(f"Error finding first CDF version: {e}")
        raise


def get_last_processed_version(target_table_path: str) -> Optional[int]:
    """
    Get the highest commit version that has been processed and written to the target table.  
    Args:
        target_table_path: Path to the target Delta table
    Returns:
        The maximum commit version (int) found in the target table, or None
    """
    if not is_table_exist(target_table_path):
        return None
    try:
        df = spark.read.format("delta").load(target_table_path)
    except AnalysisException:
        return None

    if  TARGET_CDF_COMMIT_VERSION_COLUMN not in df.columns:
        return None

    v = df.select(spark_max(col(TARGET_CDF_COMMIT_VERSION_COLUMN)).alias("v")).collect()[0]["v"]
    return int(v) if v is not None else None


def get_commit_timestamp_for_version(source_table_path: str, version: int):
    """
    Retrieve the commit timestamp for a specific version of the Delta table.
    Args:
        source_table_path: Path to the source Delta table
        version: The Delta table version number to look up
    Returns:
        Timestamp: The commit timestamp for the specified version
    """
    dt = DeltaTable.forPath(spark, source_table_path)
    row = (
        dt.history()
          .filter(col("version") == lit(int(version)))
          .select("timestamp")
          .first()
    )
    if not row or row["timestamp"] is None:
        raise ValueError(f"No commit timestamp found for version {version} at {source_table_path}")
    return row["timestamp"] 
    

def is_initial_run(target_table_path: str) -> bool:
    """    
    Determine if this is the first time processing data to the target table.
    Args:
        target_table_path: Path to the target Delta table
    Returns:
        True if no data has been processed yet, False otherwise
    """
    return get_last_processed_version(target_table_path) is None

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform helpers

# COMMAND ----------

def add_cdf_metadata_fields(
    df: DataFrame, 
    version: int, 
    commit_timestamp: datetime = None
) -> DataFrame:
    """
    Add CDF metadata fields to a DataFrame.
    Used for snapshot loads where CDF metadata needs to be manually created. 
    Args:
        df: The source DataFrame to enrich with CDF metadata
        version: The Delta table version number to record in _commit_version
        commit_timestamp: The commit timestamp from the Delta table history.                    
    Returns:
        DataFrame with the following columns added:
        - _change_type: Type of change (set to default value for snapshots)
        - _commit_timestamp: Original Delta commit timestamp
        - _commit_version: Delta table version number
        - processing_timestamp: When this pipeline run processed the data
        - processing_date: Date partition for the processing timestamp
        - source_system: Identifier for the source system
    
    """
    if commit_timestamp is None:
        raise ValueError("commit_timestamp is required for CDF metadata")
    
    logger.info(
        f"Adding CDF metadata fields (version: {version}, "
        f"commit_timestamp: {commit_timestamp})"
    )
    
    return (
        df
        .withColumn(SOURCE_CDF_CHANGE_TYPE_COLUMN, lit(DEFAULT_CHANGE_TYPE))
        .withColumn(SOURCE_CDF_COMMIT_TIMESTAMP_COLUMN, lit(commit_timestamp))
        .withColumn(SOURCE_CDF_COMMIT_VERSION_COLUMN, lit(version).cast(LongType()))
        .withColumn(PROCESSING_TIMESTAMP_COLUMN, current_timestamp())
        .withColumn(PROCESSING_DATE_COLUMN, current_date())
        .withColumn(SOURCE_SYSTEM_COLUMN, lit(SOURCE_SYSTEM))
    )


def add_pipeline_run_metadata(df: DataFrame) -> DataFrame:
    """
    Add pipeline run metadata fields to DataFrame. This is used for partitioning the target table. 
    Args:
        df: Source DataFrame    
    Returns:
        DataFrame with pipeline run metadata fields added
    Note:
        This function checks for the existence of processing_timestamp column to
        determine if metadata has already been added, preventing duplicate processing.
        If processing_timestamp exists, the function assumes all metadata fields
        are already present and returns the DataFrame unchanged.
    """
    if PROCESSING_TIMESTAMP_COLUMN not in df.columns:
        logger.info(f"Adding pipeline run metadata fields")
        enriched_df = df \
            .withColumn(PROCESSING_TIMESTAMP_COLUMN, current_timestamp()) \
            .withColumn(PROCESSING_DATE_COLUMN, current_date())\
            .withColumn(SOURCE_SYSTEM_COLUMN, lit(SOURCE_SYSTEM))
        return enriched_df
    else:
        logger.info(f"Pipeline run metadata fields already exist")
        return df
    

def prepare_cdf_for_ingestion(df: DataFrame) -> DataFrame:
    """
    Standardizes the Delta Change Data Feed (CDF) output.

    1. Removes records where `_change_type` is `"update_preimage"`, since these
       represent the old values before an update.
    2. Renames the raw CDF metadata columns
    3. Normalizes `_change_type` values:
       - `"update_postimage"` is converted to `"update"`
    Args:
       df : DataFrame
    Returns
       A standardized DataFrame with filtered records, renamed columns and normalized `_change_type` values.
    """
    logger.info("Standardizing CDF metadata fields")

    enriched_df = (
        df
        # 1. Remove pre-image rows (old values before update)
        .filter(col(SOURCE_CDF_CHANGE_TYPE_COLUMN) != "update_preimage")

        # 2. Normalize the change_type values
        .withColumn(
            SOURCE_CDF_CHANGE_TYPE_COLUMN,
            when(col(SOURCE_CDF_CHANGE_TYPE_COLUMN) == "update_postimage", "update")
            .otherwise(col(SOURCE_CDF_CHANGE_TYPE_COLUMN))
        )

        # 3. Rename raw CDF metadata columns
        .withColumnRenamed(SOURCE_CDF_CHANGE_TYPE_COLUMN, TARGET_CDF_CHANGE_TYPE_COLUMN)
        .withColumnRenamed(SOURCE_CDF_COMMIT_TIMESTAMP_COLUMN, TARGET_CDF_COMMIT_TIMESTAMP_COLUMN)
        .withColumnRenamed(SOURCE_CDF_COMMIT_VERSION_COLUMN, TARGET_CDF_COMMIT_VERSION_COLUMN)
    )
    enriched_df.display()
    return enriched_df


# COMMAND ----------

# MAGIC %md
# MAGIC #### Readers

# COMMAND ----------

def read_snapshot_before_cdf(source_table_path: str, cdf_enabled_version: int) -> DataFrame:
    """
    Read snapshot of data from before CDF was enabled.
    This reads the table state at the version just before CDF was turned on.
    Args:
        source_table_path: Path to source Delta table
        cdf_enabled_version: Version where CDF was enabled    
    Returns:
        DataFrame with snapshot data and CDF metadata
    """
    # Read the version just before CDF was enabled
    read_version = max(0, cdf_enabled_version - 1)
    
    logger.info(f"Reading snapshot at version {read_version} (before CDF version {cdf_enabled_version})")
    
    snapshot_df = spark.read \
        .format("delta") \
        .option("versionAsOf", read_version) \
        .load(source_table_path)
    
    # Add CDF metadata fields
    boundary_ts = get_commit_timestamp_for_version(source_table_path, read_version)
    enriched_df = add_cdf_metadata_fields(snapshot_df, version=read_version, commit_timestamp=boundary_ts)
    
    logger.info(f"Snapshot read complete. Records: {enriched_df.count()}")
    return enriched_df


def read_cdf_changes(source_table_path: str, start_version: int, end_version: int) -> DataFrame:
    """
    Read CDF changes from source Delta table starting from a specific version.
    This reads the actual change data feed.
    Args:
        source_table_path: Path to source Delta table
        start_version: Starting version for CDF read (inclusive)    
    Returns:
        DataFrame with CDF changes
    """
    logger.info(f"Reading CDF changes starting from version {start_version}")
    
    changes_df = spark.read \
        .format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", start_version) \
        .option("endingVersion", end_version) \
        .load(source_table_path)
    
    logger.info(f"CDF changes read complete. Records: {changes_df.count()}")
    return changes_df


# COMMAND ----------

# MAGIC %md
# MAGIC #### Writers

# COMMAND ----------

"""
Data writing operations for CDF pipeline.
"""

def write_to_target(df: DataFrame, target_table_path: str, mode: str = "append") -> None:
    """
    Write DataFrame to a target Delta table with pipeline metadata and partitioning.    
    Args:
        df: DataFrame to write
        target_table_path: Path to target Delta table
        mode: Write mode ('overwrite' or 'append')
    """
    enriched_df = prepare_cdf_for_ingestion(df)
    df_with_timestamp = add_pipeline_run_metadata(enriched_df)
    
    # logger.info(f"Writing {df_with_timestamp.count()} records to {target_table_path} in '{mode}' mode")
    logger.info(f"Writing data to {target_table_path} in '{mode}' mode")
    logger.info(f"Partitioning by: {PROCESSING_DATE_COLUMN} and {PROCESSING_TIMESTAMP_COLUMN}")
    
    df_with_timestamp.write \
        .format("delta") \
        .mode(mode) \
        .option('mergeSchema', 'true') \
        .option('schemaEvolution', 'true') \
        .partitionBy(PROCESSING_DATE_COLUMN, PROCESSING_TIMESTAMP_COLUMN) \
        .save(target_table_path)

    if mode == "overwrite":
        logger.info("Writing initial load (overwrite mode)")
    else:
        logger.info("Writing incremental load (append mode)")

    logger.info("Write operation completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingestion Pipeline orchestrator.

# COMMAND ----------

def process_initial_cdf_load(
    source_table_path: str,
    target_table_path: str,
    cdf_enabled_version: int
) -> DataFrame:
    """
    Process initial load when CDF IS enabled on source.
    Reads snapshot BEFORE CDF version (historical data) and writes to target.
    Args:
        source_table_path: Path to source Delta table
        target_table_path: Path to target Delta table
        cdf_enabled_version: Version where CDF was enabled   
    Returns:
        DataFrame with processed data
    """
    _banner("INITIAL LOAD - READ BEFORE CDF VERSION")

    # Read snapshot before CDF was enabled
    snapshot_df = read_snapshot_before_cdf(source_table_path, cdf_enabled_version)

    # Write to target (overwrite mode for first partition)
    write_to_target(snapshot_df, target_table_path, mode='overwrite')
    
    logger.info(f"Initial CDF load completed. Records written: {snapshot_df.count()}")
    return snapshot_df


def process_incremental_cdf_load(
    source_table_path: str,
    target_table_path: str,
    cdf_enabled_version: int
) -> Optional[DataFrame]:
    """
    Process incremental load with dynamic CDF processing.
    Reads CDF changes AFTER the last processed version.
    Args:
        source_table_path: Path to source Delta table
        target_table_path: Path to target Delta table
        cdf_enabled_version: Version where CDF was enabled   
    Returns:
        DataFrame with processed data (empty if no new data)
    """

    _banner("INCREMENTAL LOAD - READ AFTER CDF VERSION")
    
    # Get version information
    last_available_version = get_last_available_version(source_table_path)
    last_processed_version = get_last_processed_version(target_table_path)

    logger.info(f"Last available version in source: {last_available_version}")
    logger.info(f"Last processed version in target: {last_processed_version}")

    if last_available_version is None:
        logger.error(f"Unable to determine max version for source table: {source_table_path}")
        raise ValueError(f"Cannot get max version from source table: {source_table_path}")
    

    # Determine start and end version for CDF read
    if last_processed_version is not None and last_processed_version >= cdf_enabled_version:
        start_version = last_processed_version + 1
    else:
        start_version = cdf_enabled_version
    
    end_version = last_available_version
    
    # Check if there's new data to process
    if last_available_version <= last_processed_version if last_processed_version is not None else False:
        logger.info(
            f"No new data to process. "
            f"Last available version ({last_available_version}) <= Last processed version ({last_processed_version})"
        )
        return None

    logger.info(
        f"Reading CDF window [{start_version} … {end_version}]. Versions to process: {end_version - start_version}"
    ) 
    
    # Read CDF changes
    changes_df = read_cdf_changes(source_table_path, start_version, end_version)
    
    # Check if there are any changes
    record_count = changes_df.count()
    if record_count == 0:
        logger.info("No new changes to process (empty result from CDF read)")

    # if not _non_empty(changes_df):
    #     logger.info("No new changes to process (empty result from CDF read)")
    
    # Write to target (append mode)
    write_to_target(changes_df, target_table_path, mode='append')
    
    logger.info(f"Incremental CDF load completed. Records written: {record_count}")
    logger.info(f"Target now up-to-date through version {last_available_version}")
    
    return changes_df


def run_ingestion_pipeline(source_table_path: str, target_table_path: str) -> Optional[DataFrame]:
    """
    Main pipeline orchestrator that implements the CDF processing logic:
    
    1. Check if source table has CDF enabled
       - NO: Fail
       - YES: Continue to step 2
    
    2. Check if this is initial run
       - YES: Read data BEFORE CDF version (historical snapshot)
       - NO: Read data AFTER CDF version (incremental changes)
    
    Args:
        source_table_path: Path to source Delta table
        target_table_path: Path to target Delta table
        
    Returns:
        Processed DataFrame
    """
    _banner("STARTING PIPELINE")

    # Step 1: Check if CDF is enabled on source
    cdf_enabled = is_cdf_enabled(source_table_path)
    
    if not cdf_enabled:
        logger.error("CDF is NOT enabled")
    
    # CDF IS enabled - find the version where it was enabled
    logger.info("Decision: CDF IS enabled -> Checking if initial or incremental run")
    cdf_enabled_version = get_first_cdf_enabled_version(source_table_path)
    
    if cdf_enabled_version is None:
        logger.error("CDF is enabled but cannot find enablement version. Existing.")
        raise
    
    # Step 2: Check if this is initial run
    initial_run = is_initial_run(target_table_path)

    if initial_run:
        # YES - Initial run: Read BEFORE CDF version
        logger.info("Decision: INITIAL RUN -> Read data BEFORE CDF version")
        return process_initial_cdf_load(source_table_path, target_table_path, cdf_enabled_version)
    else:
        # NO - Subsequent run: Read AFTER CDF version (incremental)
        logger.info("Decision: SUBSEQUENT RUN -> Read data AFTER CDF version")
        return process_incremental_cdf_load(source_table_path, target_table_path, cdf_enabled_version)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Additional (Not in Use)

# COMMAND ----------

# def create_bronze_external_table(table_name: str, target_table_path: str):
#     """
#     Create a Delta table in the bronze layer if it doesn't exist.
    
#     Args:
#         table_name: Table Name
#         target_table_path: Path to target Delta table
        
#     """
#     spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS dev.bronze.brz_{table_name}
#     USING DELTA
#     LOCATION '{target_table_path}'
#     """)
    
#     logging.info(f"Table prod.bronze.{table_name} created or already exists at {target_table_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main

# COMMAND ----------

"""
Main entry point for Ingestion Pipeline execution.
"""

def main():
    """
    Execute the CDF pipeline with configured source and target paths.
    """
    logger.info(f"Source table: {SOURCE_TABLE_PATH}")
    logger.info(f"Target table: {TARGET_TABLE_PATH}")

    try:
        run_ingestion_pipeline(
            source_table_path=SOURCE_TABLE_PATH,
            target_table_path=TARGET_TABLE_PATH
        )

        # Currently Bronze table created within Lakeflow as a managed table 
        # create_bronze_external_table(table_name=TABLE_NAME, target_table_path=TARGET_TABLE_PATH)
        
        _banner("PIPELINE COMPLETED SUCCESSFULLY")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

