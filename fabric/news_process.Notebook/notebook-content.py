# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "da3f5d52-b053-455d-83b5-bf1379e1aaa7",
# META       "default_lakehouse_name": "news",
# META       "default_lakehouse_workspace_id": "8e5fb03f-6562-47c0-85de-c326bd1ee54d",
# META       "known_lakehouses": [
# META         {
# META           "id": "da3f5d52-b053-455d-83b5-bf1379e1aaa7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Read Data

# CELL ********************

from pyspark.sql import functions as f
from pyspark.sql.types import *
from datetime import date
import json
from delta.tables import *

raw_df = spark.read\
    .option('multiline', 'true')\
    .format('json')\
    .load(f'Files/raw/global-news_{date.today()}.json')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Format Data

# CELL ********************

# only process organic_results field
df_res = raw_df.select(f.explode('organic_results').alias('res_str'))

# define schema
# schema = StructType() \
#     .add("source", StringType()) \
#     .add("snippet", StringType()) \
#     .add("thumbnail", StringType()) \
#     .add("date", StringType()) \
#     .add("link", StringType()) \
#     .add("position", IntegerType()) \
#     .add("title", StringType()) \
#     .add("favicon", StringType())

# data cleaning

staged_df = df_res.select("res_str.source", "res_str.title", "res_str.snippet", "res_str.link", "res_str.thumbnail", "res_str.favicon", "res_str.date")

# format date column

cleaned_df = staged_df.withColumn(
    "date", f.to_date(
    f.when(
        f.col("date").endswith("d"),
        f.date_sub(
            f.current_date(),
            f.regexp_extract(f.col("date"), r"(\d+)", 1).cast("int")
        )
    ).when(
        f.col("date").endswith("y"),
        f.add_months(
            f.current_date(),
            -f.regexp_extract(f.col("date"), r"(\d+)", 1).cast("int") * 12
        )
    ).otherwise(f.col("date"))
    )
)
# add one additional col to track pipeline run date
cleaned_df = cleaned_df.withColumn('extracted_date', f.current_date())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write data to silver delta table in lakehouse

# CELL ********************

# create table

DeltaTable.createIfNotExists(spark)\
    .tableName('stg_bing_news')\
    .location('Files/processed/stg_bing_news')\
    .addColumn('source', StringType())\
    .addColumn('title', StringType())\
    .addColumn('snippet', StringType())\
    .addColumn('link', StringType())\
    .addColumn('thumbnail', StringType())\
    .addColumn('favicon', StringType())\
    .addColumn('orig_date', DateType())\
    .addColumn('extracted_date', DateType())\
.execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Increamental Data Load

# CELL ********************

# update existing data and insert new data
table = DeltaTable.forPath(spark, 'Files/processed/stg_bing_news')

table.alias('tb')\
    .merge(
        cleaned_df.alias('df'),
        'tb.title = df.title and tb.extracted_date = df.extracted_date'
    )\
    .whenMatchedUpdate(set={

    }
    )\
    .whenNotMatchedInsert(values={
        'source': f.col('df.source'),
        'title': f.col('df.title'),
        'snippet': f.col('df.snippet'),
        'link': f.col('df.link'),
        'thumbnail': f.col('df.thumbnail'),
        'favicon': f.col('df.favicon'),
        'orig_date': f.col('df.date'),
        'extracted_date': f.col('df.extracted_date')
    }
    )\
.execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
