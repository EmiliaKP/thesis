#%% powołanie sesji Apache Spark
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder.master("local[*]").appName("PRQT_count_cache").getOrCreate()

#%% wczytanie danych
from pyspark.sql.functions import col

df=spark.read.parquet("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source_p/ghtorrent-2019-01-07.parquet").cache()

print(df.count())

print(df.count())

print(df.filter(col('comment_id')>270000000).count())
