import dlt as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Dict, Optional, Literal

MAX_END_AT = "9999-12-31 23:59:59"

def create_scd2_table(view_name, scd2_table_name, keys, sequence_by: str = "_raw_commit_timestamp"):
    dp.create_streaming_table(scd2_table_name)
    dp.apply_changes(
        target=scd2_table_name,
        source=view_name,
        keys=keys,
        sequence_by=F.col(sequence_by),
        apply_as_deletes=F.expr("_raw_change_type = 'delete'"),
        stored_as_scd_type=2
    )

def create_scd1_table(view_name, scd1_table_name, keys, sequence_by: str = "_raw_commit_timestamp"):
    dp.create_streaming_table(scd1_table_name)
    dp.apply_changes(
        target=scd1_table_name,
        source=view_name,
        keys=keys,
        sequence_by=F.col(sequence_by),
        apply_as_deletes=F.expr("_raw_change_type = 'delete'"),
        stored_as_scd_type=1
    )

# def create_scd2_materialized_view_simple(scd2_table_name, scd2_materialized_view_name):
#     @dp.table(name=scd2_materialized_view_name)
#     def mv():
#         return dp.read(scd2_table_name) \
#                 .withColumn("is_current", F.col("__END_AT").isNull()) \
#                 .withColumn("__END_AT",
#                     F.when(
#                         F.col("__END_AT").isNull(),
#                         F.lit(MAX_END_AT)
#                     ).otherwise(F.col("__END_AT"))
#                 )

def create_scd2_materialized_view(
    scd2_table_name,
    scd2_materialized_view_name,
    keys
):
    @dp.table(name=scd2_materialized_view_name)
    def mv():
        df = dp.read(scd2_table_name)
        
        window_spec = Window.partitionBy(keys).orderBy(
            F.col("__START_AT").desc(),
            F.col("__END_AT").desc_nulls_last()
        )

        return (
            df
            .withColumn(
                "is_deleted",
                F.first(
                    (F.col("_raw_change_type") == 'delete').cast("boolean"),
                    ignorenulls=True
                ).over(window_spec)
            )
            .withColumn("is_current", F.col("__END_AT").isNull())
            .withColumnRenamed("__START_AT", "_row_valid_from")
            .withColumn(
                "_row_valid_to",
                F.coalesce(F.col("__END_AT"), F.lit(MAX_END_AT))
            )
            .drop("__END_AT")
        )