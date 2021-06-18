#%% powołanie sesji Apache Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CSV_distinct_cache").getOrCreate()

#%% wczytanie danych
from pyspark.sql.functions import col
# wczytanie wszystkich plików CSV w danym folderze
df_cached=spark.read.options(header='true',delimiter=',', inferSchema='true')\
    .csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source")\
    .select('language','author_login','actor_login')\
    .filter(col('comment_id')>270000000).cache()

#%% stworzenie df z unikatowymi wartościami
df_lang=df_cached.select(df_cached.language).distinct()
df_author=df_cached.select(df_cached.author_login).distinct()
df_actor=df_cached.select(df_cached.actor_login).distinct()

#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_lang.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_distinct_lang.csv", header=True)
df_author.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_distinct_author.csv", header=True)
df_actor.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_distinct_actor.csv", header=True)
print("Dane zapisane do plików.")

