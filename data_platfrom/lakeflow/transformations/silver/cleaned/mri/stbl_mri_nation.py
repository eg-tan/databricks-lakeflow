from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

from utilities.functions.common_functions import get_business_key, get_silver_metadata_columns
from utilities.helpers.silver_scd_table_creator import create_silver_scd_table

ENTITY_NAME = 'mri_nation'

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
            .table("bronze.stbl_mri_nation")
            .select(
                
                # Business keys
                get_business_key(["n_nationkey", "_source_system"]),

                # Core attributes
                F.col("n_nationkey").cast(IntegerType()).alias("n_nationkey"),
                F.col("n_name").cast(StringType()).alias("n_name"),
                F.col("n_regionkey").cast(IntegerType()).alias("n_regionkey"),
                F.col("n_comment").cast(IntegerType()).alias("n_comment"),

                # Additional Metadata
                *get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Type 2 Implementation 
# ==========================================================

create_silver_scd_table(
    entity_name=ENTITY_NAME,
    business_keys=["n_nationkey"],
    scd_type=2
)
