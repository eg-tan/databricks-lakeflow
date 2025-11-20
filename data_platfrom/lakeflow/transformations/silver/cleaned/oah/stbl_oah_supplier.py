from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

from utilities.functions.common_functions import get_business_key, get_silver_metadata_columns
from utilities.helpers.silver_scd_table_creator import create_silver_scd_table

ENTITY_NAME = 'oah_supplier'

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
            .table("bronze.stbl_oah_supplier")
            .select(
                
                # Business keys
                get_business_key(["s_suppkey", "_source_system"]),

                # Core attributes
                F.col("s_suppkey").cast("int").alias("s_suppkey"),
                F.col("s_name").cast("string").alias("s_name"),
                F.col("s_address"),
                F.col("s_nationkey").cast("int").alias("s_nationkey"),
                F.col("s_phone").cast("string").alias("s_phone"),
                F.col("s_acctbal").cast("double").alias("s_acctbal"),
                F.col("s_comment"),

                # Additional Metadata
                *get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Type 2 Implementation 
# ==========================================================

create_silver_scd_table(
    entity_name=ENTITY_NAME,
    business_keys=["s_suppkey"],
    scd_type=2
)
