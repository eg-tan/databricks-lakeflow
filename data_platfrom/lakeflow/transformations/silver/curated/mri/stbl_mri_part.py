from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

import utilities.functions.common_functions as utils
import utilities.helpers.silver_scd_creator as scd_utils

# ==========================================================
# TEMP VIEW: Cleaned Source Data
# ==========================================================

@dp.temporary_view(
    name=f"vw_mri_part_cleaned",
    comment="..."
)
def create_temp_view():

    return (
        spark.readStream
            .table("admin_bronze.stbl_mri_part")
            .select(
                
                # Business keys
                utils.get_business_key(["p_partkey", "_source_system"]),

                # Core attributes
                F.col("p_partkey").cast("int").alias("p_partkey"),
                F.col("p_name"),
                F.col("p_mfgr").cast("string").alias("p_mfgr"),
                F.col("p_brand").cast("string").alias("p_brand"),
                F.col("p_type"),
                F.col("p_size").cast("int").alias("p_size"),
                F.col("p_container").cast("string").alias("p_container"),
                F.col("p_retailprice").cast("int").alias("p_retailprice"),
                F.col("p_comment"),

                # Additional Metadata
                *utils.get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Type 2 Implementation 
# ==========================================================

scd_utils.create_scd2_table(
    view_name='vw_mri_part_cleaned',
    scd2_table_name="stbl_mri_part_scd2",
    keys=["p_partkey"]
)
