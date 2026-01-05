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
import synapse.ml.core
from synapse.ml.services import AnalyzeText
from delta.tables import *
from pyspark.sql.types import *

df = spark.sql("SELECT * FROM stg_bing_news")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Make model

# CELL ********************

model = (AnalyzeText()
        .setTextCol("snippet")
        .setKind("SentimentAnalysis")
        .setOutputCol("response")
        .setErrorCol("error"))

res = model.transform(df)

res_df = res.withColumn('sentiment', f.col('response.documents.sentiment'))

final_df = res_df.drop('error', 'response')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Gold Table 

# CELL ********************

#create table

DeltaTable.createIfNotExists(spark)\
    .tableName('bing')\
    .location('Files/presentation/bing')\
    .addColumn('source', StringType())\
    .addColumn('title', StringType())\
    .addColumn('snippet', StringType())\
    .addColumn('link', StringType())\
    .addColumn('thumbnail', StringType())\
    .addColumn('favicon', StringType())\
    .addColumn('orig_date', DateType())\
    .addColumn('extracted_date', DateType())\
    .addColumn('sentiment', StringType())\
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
table = DeltaTable.forPath(spark, 'Files/presentation/bing')

table.alias('tb')\
    .merge(
        final_df.alias('df'),
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
        'orig_date': f.col('df.orig_date'),
        'extracted_date': f.col('df.extracted_date'),
        'sentiment': f.col('sentiment')
    }
    )\
.execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
