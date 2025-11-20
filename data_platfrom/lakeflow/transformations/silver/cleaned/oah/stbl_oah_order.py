from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

from utilities.functions.common_functions import get_business_key, get_silver_metadata_columns
from utilities.helpers.silver_scd_table_creator import create_silver_scd_table


ENTITY_NAME = 'oah_order'

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
            .table("bronze.stbl_oah_order")
            .select(
                
                # Business keys
                get_business_key(["o_orderkey", "_source_system"]),

                # Core attributes
                F.col("o_orderkey").cast("int").alias("o_orderkey"),
                F.col("o_custkey").cast("int").alias("o_custkey"),
                F.substring(F.col("o_orderstatus"), 1, 1).cast("string").alias("o_orderstatus"),
                F.col("o_totalprice").cast("double").alias("o_totalprice"),
                F.col("o_orderdate").cast("date").alias("o_orderdate"),
                F.col("o_orderpriority").cast("string").alias("o_orderpriority"),
                F.col("o_clerk").cast("string").alias("o_clerk"),
                F.col("o_shippriority").cast("int").alias("o_shippriority"),
                F.col("o_comment"),

                # Additional Metadata
                *get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Type 2 Implementation 
# ==========================================================

create_silver_scd_table(
    entity_name=ENTITY_NAME,
    business_keys=["o_orderkey"],
    scd_type=2
)