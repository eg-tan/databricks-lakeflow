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
    name=f"vw_mri_region_cleaned",
    comment="..."
)
def create_temp_view():

    return (
        spark.readStream
            .table(f"{bronze_schema}.stbl_mri_region")
            .select(
                
                # Business keys
                utils.get_business_key(["r_regionkey", "_source_system"]),

                # Core attributes
                F.col("r_regionkey"),
                F.col("r_name").cast("string").alias("r_name"),
                F.col("r_comment"),

                # Additional Metadata
                *utils.get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Type 2 Implementation 
# ==========================================================

scd_utils.create_scd2_table(
    view_name='vw_mri_region_cleaned',
    scd2_table_name="stbl_mri_region_hist",
    keys=["r_regionkey"]
)
