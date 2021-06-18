#%% powołanie sesji Apache Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("PRQT_filtr").getOrCreate()

#%% wczytanie danych
df=spark.read.parquet("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source_p/ghtorrent-2019-01-07.parquet")

#%% df pofiltrowane po Pythonie
df_Python=df.filter(df.language=="Python")

#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_Python.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_filtr_Python.csv", header=True)
print("Dane zapisane do plików.")
