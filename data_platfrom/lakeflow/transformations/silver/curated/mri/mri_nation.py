from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

import utilities.functions.common_functions as utils
import utilities.helpers.silver_scd_creator as scd_utils

bronze_schema = spark.conf.get("pipeline.bronze_schema")

#==========================================================
# TEMP VIEW: Cleaned Source Data
#==========================================================

@dp.temporary_view(
    name=f"vw_mri_nation_cleaned",
    comment="..."
)
def create_temp_view():

    return (
        spark.readStream
            .table(f"{bronze_schema}.stbl_mri_nation")
            .select(
                
                # Business keys
                utils.get_business_key(["n_nationkey", "_source_system"]),

                # Core attributes
                F.col("n_nationkey").cast(IntegerType()).alias("n_nationkey"),
                F.col("n_name").cast(StringType()).alias("n_name"),
                F.col("n_regionkey").cast(IntegerType()).alias("n_regionkey"),
                F.col("n_comment").cast(IntegerType()).alias("n_comment"),

                # Additional Metadata
                *utils.get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Type 2 Implementation 
# ==========================================================

scd_utils.create_scd2_table(
    view_name='vw_mri_nation_cleaned',
    scd2_table_name="stbl_mri_nation_hist",
    keys=["n_nationkey"]
)

scd_utils.create_scd2_materialized_view(
    scd2_table_name='stbl_mri_nation_hist',
    scd2_materialized_view_name="mvw_mri_nation_scd2_hist",
    keys=["n_nationkey"]
)