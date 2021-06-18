from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CSV_filtr_cache").getOrCreate()

#%% wczytanie danych
from pyspark.sql.functions import col
# wczytanie wszystkich plików CSV w danym folderze
df_cached=spark.read.options(header='true',delimiter=',', inferSchema='true')\
    .csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source")\
    .select('language','comment').filter(col('comment_id')>270000000).cache()
    
#%% stworzenie df przy użyciu różnych wyszukiwań
df_Python=df_cached.filter(df_cached.language=="Python")
df_C=df_cached.filter(df_cached.language=="C++")
df_NotNull=df_cached.filter(df_cached.language.isNotNull())


#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_Python.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_filtr_Python_cache.csv", header=True)
df_C.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_filtr_C_cache.csv", header=True)
df_NotNull.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_filtr_NotNull_cache.csv", header=True)
print("Dane zapisane do plików.")
df_cached.unpersist()
print("Pamięc wyczyszczona")
