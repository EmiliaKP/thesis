from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CSV_grup_cache").getOrCreate()

#%% wczytanie danych
from pyspark.sql.functions import col
# wczytanie wszystkich plików CSV w danym folderze
df_cached=spark.read.options(header='true',delimiter=',', inferSchema='true')\
    .csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source")\
    .select('language','repo','author_login').filter(col('comment_id')>270000000).cache()

#%% stworzenie kilku df grupujących
df_repo_lang=df_cached.groupBy(df_cached.repo, df_cached.language).count()
df_lang=df_cached.groupBy(df_cached.language).count()
df_repo=df_cached.groupBy(df_cached.repo).count()
df_author=df_cached.groupBy(df_cached.author_login).count()

#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_repo_lang.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_grup_repo_lang_cache.csv", header=True)
df_lang.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_grup_lang_cache.csv", header=True)
df_repo.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_grup_repo_cache.csv", header=True)
df_author.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_grup_author_cache.csv", header=True)

print("Dane zapisane do plików.")
df_cached.unpersist()
print("Pamięc wyczyszczona")
