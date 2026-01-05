# bing-news-analysis
etl/elt, fabric

# Prerequisite
1) MS Fabric with atleast trial capacity
2) SearchApi free tier

### Steps to replicate or initiate this project
1) Create a workspace for bing-news-analysis in microsoft fabric
2) Create a lakehouse with public preview disabled

### Pipeline
1) Create a rest connection inside Copy Data activity to get data from source to raw folder in lakehouse files and provide this string "@concat('global-news', '_',substring(utcNow(), 0, 10), '.json')" in file name
2) Add notebook news_process to copy data activity
3) Add sentiment_analysis notebook to prev notebook
4) Schedule, monitor pipeline daily
