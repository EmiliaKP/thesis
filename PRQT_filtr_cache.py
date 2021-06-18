#%% powołanie sesji Apache Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("PRQT_filtr_cache").getOrCreate()

#%% wczytanie danych
from pyspark.sql.functions import col

df_cached=spark.read.parquet("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source_p/ghtorrent-2019-01-07.parquet")\
    .select('language','author_login','actor_login')\
    .filter(col('comment_id')>270000000).cache()

#%% stworzenie df przy użyciu różnych wyszukiwań
df_Python=df_cached.filter(df_cached.language=="Python")
df_C=df_cached.filter(df_cached.language=="C++")
df_NotNull=df_cached.filter(df_cached.language.isNotNull())

#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_Python.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_filtr_Python.csv", header=True)
df_C.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_filtr_Python.csv", header=True)
df_NotNull.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_filtr_Python.csv", header=True)
print("Dane zapisane do plików.")

