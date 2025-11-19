# Databricks notebook source
dbutils.widgets.text(
    "CONFIG_PATH",
    "...",
    "Config path"
)

CONFIG_PATH = dbutils.widgets.get("CONFIG_PATH").strip()

# COMMAND ----------

import yaml

with open(f"{CONFIG_PATH}", "r") as f:
    config = yaml.safe_load(f)

# Transform YAML config to match the expected format (if needed)
table_configs = []
for table in config['tables']:
    table_config = {
        "TABLE_NAME": table['table_name'],
        "SOURCE_SYSTEM": config['source_system'],
        "SOURCE_TABLE_PATH": f"{config['source_system_path']}/{table['table_name'].replace('brz_', 'h_')}",
        "TARGET_TABLE_PATH": f"{config['target_system_path']}/{table['table_name']}"
    }
    table_configs.append(table_config)

# Set as task value
dbutils.jobs.taskValues.set(key="table_configs", value=table_configs)
