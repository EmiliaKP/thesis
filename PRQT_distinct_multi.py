#%% powołanie sesji Apache Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]")\
.appName("PRQT_distinct_multi").getOrCreate()

#%% wczytanie danych
from pyspark.sql.functions import col

df=spark.read.parquet("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source_p/ghtorrent-2019-01-07.parquet")\
    .select('language','author_login','actor_login').filter(col('comment_id')>270000000)

#%% stworzenie df z unikatowymi wartościami
df_lang=df.select(df.language).distinct()
df_author=df.select(df.author_login).distinct()
df_actor=df.select(df.actor_login).distinct()

#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_lang.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_distinct.csv", header=True)
df_author.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_distinct.csv", header=True)
df_actor.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_distinct.csv", header=True)
print("Dane zapisane do plików.")
