from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

from utilities.functions.common_functions import get_business_key, get_silver_metadata_columns
from utilities.helpers.silver_scd_table_creator import create_silver_scd_table

ENTITY_NAME = 'mri_customer'

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
            .table("bronze.stbl_mri_customer")
            .select(
                
                # Business keys
                get_business_key(["c_custkey", "_source_system"]),

                # Core attributes
                F.col("c_custkey").cast("int").alias("c_custkey"),
                F.col("c_name"),
                F.col("c_address"),
                F.col("c_nationkey").cast("int").alias("c_nationkey"),
                F.col("c_phone").cast("string").alias("c_phone"),
                F.col("c_acctbal").cast("double").alias("c_acctbal"),
                F.col("c_mktsegment").cast("string").alias("c_mktsegment"),
                F.col("c_comment"),

                # Additional Metadata
                *get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Type 2 Implementation 
# ==========================================================


create_silver_scd_table(
    entity_name=ENTITY_NAME,
    business_keys=["c_custkey"],
    scd_type=2
)
