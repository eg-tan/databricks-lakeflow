from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

import utilities.functions.common_functions as utils
import utilities.helpers.silver_scd_creator as scd_utils

bronze_schema = spark.conf.get("pipeline.bronze_schema")

# ==========================================================
# TEMP VIEW: Cleaned Source Data
# ==========================================================

@dp.temporary_view(
    name=f"vw_oah_order_cleaned",
    comment="..."
)
def create_temp_view():

    return (
        spark.readStream
            .table(f"{bronze_schema}.stbl_oah_order")
            .select(
                
                # Business keys
                utils.get_business_key(["o_orderkey", "_source_system"]),

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
                *utils.get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: Implementation 
# ==========================================================

scd_utils.create_scd1_table(
    view_name='vw_oah_order_cleaned',
    scd1_table_name="stbl_oah_order_hist",
    keys=["o_orderkey"]
)
