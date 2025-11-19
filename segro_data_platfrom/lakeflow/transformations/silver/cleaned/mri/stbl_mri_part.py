from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

from utilities.functions.common_functions import get_business_key, get_silver_metadata_columns
from utilities.helpers.silver_scd_table_creator import create_silver_scd_table

ENTITY_NAME = 'mri_part'

# ==========================================================
# TEMP VIEW: Cleaned Source Data
# ==========================================================

@dp.temporary_view(
    name=f"vw_{ENTITY_NAME}_cleaned",
    comment="..."
)
def create_temp_view():

    return (
        spark.readStream
            .table("bronze.stbl_mri_part")
            .select(
                
                # Business keys
                get_business_key(["p_partkey", "_source_system"]),

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
                *get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Type 2 Implementation 
# ==========================================================

create_silver_scd_table(
    entity_name=ENTITY_NAME,
    business_keys=["p_partkey"],
    scd_type=2
)
