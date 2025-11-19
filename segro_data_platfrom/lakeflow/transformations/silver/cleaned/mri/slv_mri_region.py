from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

from utilities.functions.common_functions import get_business_key, get_silver_metadata_columns
from utilities.helpers.silver_scd_stream_builder import create_silver_scd_table

ENTITY_NAME = 'mri_region'

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
            .table("bronze.stbl_mri_region")
            .select(
                
                # Business keys
                get_business_key(["r_regionkey", "_source_system"]),

                # Core attributes
                F.col("r_regionkey"),
                F.col("r_name").cast("string").alias("r_name"),
                F.col("r_comment"),

                # Additional Metadata
                *get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Type 2 Implementation 
# ==========================================================

create_silver_scd_table(
    entity_name=ENTITY_NAME,
    business_keys=["r_regionkey"],
    scd_type=2
)
