from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CSV_filtr_multi").getOrCreate()

#%% wczytanie danych
from pyspark.sql.functions import col
# wczytanie wszystkich plików CSV w danym folderze
df=spark.read.options(header='true',delimiter=',', inferSchema='true')\
    .csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source")\
    .select('language','comment').filter(col('comment_id')>270000000)

#%% stworzenie df przy użyciu różnych wyszukiwań
df_Python=df.filter(df.language=="Python")
df_C=df.filter(df.language=="C++")
df_NotNull=df.filter(df.language.isNotNull())


#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_Python.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_filtr_Python.csv", header=True)
df_C.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_filtr_C.csv", header=True)
df_NotNull.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_filtr_NotNull.csv", header=True)
print("Dane zapisane do plików.")
