from utilities.helpers import bronze_table_creator as bronze_utils

bronze_utils.create_bronze_table(
    spark,
    table_name="stbl_mri_nation",
    source_path="raw_delta_hist/mri/h_nation"
)