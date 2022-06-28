import pyspark.sql
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


conf = SparkConf()

conf.set('google.cloud.auth.service.account.enable', 'true')
conf.set("temporaryGcsBucket","gcs_bucket")
conf.set("viewsEnabled","true")
conf.set('bigQueryJobLabel.owner', LABEL_OWNER)  #add labels in the jobs, used for adding labels while creati


spark = SparkSession.builder.config(conf=conf).appName("pyspark-bigquery-saveas").getOrCreate()

df = spark.read.format('bigquery').load('dataset.tablename').cache()

sql = """
  SELECT tag, COUNT(*) c
  FROM (
    SELECT SPLIT(tags, '|') tags
    FROM `bigquery-public-data.stackoverflow.posts_questions` a
    WHERE EXTRACT(YEAR FROM creation_date)>=2014
  ), UNNEST(tags) tag
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 10
  """
df_sql = spark.read.format("bigquery").load(sql)
#or spark.read.format("bigquery").option("query",sql).load()
#set "viewEnable" true, and "materializationDataset" must be set to  dataset where the GCP user has table creation permission

df.write.format('bigquery').\
    mode('overwrite').\
    option('createDisposition', 'CREATE_IF_NEEDED').\
    save('dataset.tablename')

#mode: 
#"error"- when saving a DataFrame to a data source, if data already exists, an exception is expected to be thrown.
#"append"- when saving a DataFrame to a data source,if data/table already exists, content of the DataFrame are expected to be appended to existsing data
#"overwrite"-  Overwrite mode means that when saving a DataFrame to a data source, if data/table already exists, existing data is expected to be overwritten by the content of the DataFrame
#"ignore"- Ignore mode means that when saving a DataFrame to a data source, if data already exists, the save operation is expected not to save the contents of the DataFrame and not to change the existing data. This is similar to a CREATE TABLE IF NOT EXISTS in SQL.



#options
#createDisposition:- CREATE_IF_NEEDED- create the table if it doesn't exist.
#CREATE_NEVER:- job will fail if the table doesn't exist.
