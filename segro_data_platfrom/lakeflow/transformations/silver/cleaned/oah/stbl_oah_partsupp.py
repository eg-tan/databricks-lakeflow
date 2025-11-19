from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

from utilities.functions.common_functions import get_business_key, get_silver_metadata_columns
from utilities.helpers.silver_scd_table_creator import create_silver_scd_table


ENTITY_NAME = 'oah_partsupp'

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
            .table("bronze.stbl_oah_partsupp")
            .select(
                
                # Business keys
                get_business_key(["ps_partkey", "ps_suppkey", "_source_system"]),

                # Core attributes
                F.col("ps_partkey").cast("int").alias("ps_partkey"),
                F.col("ps_suppkey").cast("int").alias("ps_suppkey"),
                F.col("ps_availqty").cast("int").alias("ps_availqty"),
                F.col("ps_supplycost").cast("double").alias("ps_supplycost"),
                F.col("ps_comment"),

                # Additional Metadata
                *get_silver_metadata_columns()

            )
    )


# ==========================================================
# TABLE: SCD Type 2 Implementation 
# ==========================================================

create_silver_scd_table(
    entity_name=ENTITY_NAME,
    business_keys=["ps_partkey", "ps_suppkey"],
    scd_type=2
)
