#%% powołanie sesji Apache Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CSV_distinct").getOrCreate()

#%% wczytanie danych
df=spark.read.options(header='true',delimiter=',', inferSchema='true')\
    .csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source")

#%% df z unikatowymi wartościami kolumny df.language
df_lang=df.select(df.language).distinct()

#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_lang.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_distinct.csv", header=True)
print("Dane zapisane do plików.")
