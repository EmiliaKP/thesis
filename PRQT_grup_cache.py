#%% powołanie sesji Apache Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("PRQT_grup_cache").getOrCreate()

#%% wczytanie danych
from pyspark.sql.functions import col

df_cached=spark.read.parquet("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source_p/ghtorrent-2019-01-07.parquet")\
    .select('language','author_login','actor_login', 'repo')\
   .filter(col('comment_id')>270000000).cache()

#%% stworzenie kilku df grupujących
df_lang=df_cached.groupBy(df_cached.language).count()
df_repo_lang=df_cached.groupBy(df_cached.repo, df_cached.language).count()
df_repo=df_cached.groupBy(df_cached.repo).count()
df_author=df_cached.groupBy(df_cached.author_login).count()

#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_lang.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_grup.csv", header=True)
df_repo_lang.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_grup.csv", header=True)
df_repo.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_grup.csv", header=True)
df_author.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_grup.csv", header=True)
print("Dane zapisane do plików.")
