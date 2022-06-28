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

df.write.format('bigquery').\
    mode('overwrite').\
    option('createDisposition', 'CREATE_IF_NEEDED').\
    save('dataset.tablename')
