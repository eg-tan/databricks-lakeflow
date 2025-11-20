from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

import utilities.functions.common_functions as utils
import utilities.helpers.silver_scd_creator as scd_utils

# ==========================================================
# TEMP VIEW: Cleaned Source Data
# ==========================================================

@dp.temporary_view(
    name=f"vw_oah_partsupp_cleaned",
    comment="..."
)
def create_temp_view():

    return (
        spark.readStream
            .table("admin_bronze.stbl_oah_partsupp")
            .select(
                
                # Business keys
                utils.get_business_key(["ps_partkey", "ps_suppkey", "_source_system"]),

                # Core attributes
                F.col("ps_partkey").cast("int").alias("ps_partkey"),
                F.col("ps_suppkey").cast("int").alias("ps_suppkey"),
                F.col("ps_availqty").cast("int").alias("ps_availqty"),
                F.col("ps_supplycost").cast("double").alias("ps_supplycost"),
                F.col("ps_comment"),

                # Additional Metadata
                *utils.get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Type 2 Implementation 
# ==========================================================

scd_utils.create_scd2_table(
    view_name='vw_oah_partsupp_cleaned',
    scd2_table_name="stbl_oah_partsupp_scd2",
    keys=["ps_partkey", "ps_suppkey"]
)

