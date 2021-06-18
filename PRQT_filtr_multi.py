#%% powołanie sesji Apache Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("PRQT_filtr_multi").getOrCreate()

#%% wczytanie danych
from pyspark.sql.functions import col

df=spark.read.parquet("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source_p/ghtorrent-2019-01-07.parquet")\
    .select('language','author_login','actor_login').filter(col('comment_id')>270000000)

#%% stworzenie df przy użyciu różnych filtrów
df_Python=df.filter(df.language=="Python")
df_C=df.filter(df.language=="C++")
df_NotNull=df.filter(df.language.isNotNull())

#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_Python.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_filtr_Python.csv", header=True)
df_C.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_filtr_Python.csv", header=True)
df_NotNull.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_filtr_Python.csv", header=True)
print("Dane zapisane do plików.")

