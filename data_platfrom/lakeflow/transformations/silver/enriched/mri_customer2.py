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
    name=f"vw_mri_customer2_cleaned",
    comment="..."
)
def create_temp_view():

    return (
        spark.readStream
            .table("{bronze_schema}.stbl_mri_customer")
            .select(
                
                # Business keys
                utils.get_business_key(["c_custkey", "_source_system"]),

                # Core attributes
                F.col("c_custkey").cast("int").alias("c_custkey"),
                F.col("c_name"),

                # Additional Metadata
                *utils.get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Implementation 
# ==========================================================


scd_utils.create_scd2_table(
    view_name='vw_mri_customer2_cleaned',
    scd2_table_name="stbl_mri_customer2_hist",
    keys=["c_custkey"]
)

