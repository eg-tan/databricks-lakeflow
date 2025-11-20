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
    name=f"vw_oah_supplier_cleaned",
    comment="..."
)
def create_temp_view():

    return (
        spark.readStream
            .table(f"{bronze_schema}.stbl_oah_supplier")
            .select(
                
                # Business keys
                utils.get_business_key(["s_suppkey", "_source_system"]),

                # Core attributes
                F.col("s_suppkey").cast("int").alias("s_suppkey"),
                F.col("s_name").cast("string").alias("s_name"),
                F.col("s_address"),
                F.col("s_nationkey").cast("int").alias("s_nationkey"),
                F.col("s_phone").cast("string").alias("s_phone"),
                F.col("s_acctbal").cast("double").alias("s_acctbal"),
                F.col("s_comment"),

                # Additional Metadata
                *utils.get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Type 2 Implementation 
# ==========================================================

scd_utils.create_scd2_table(
    view_name='vw_oah_supplier_cleaned',
    scd2_table_name="stbl_oah_supplier_hist",
    keys=["s_suppkey"]
)
