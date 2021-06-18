#%% powołanie sesji Apache Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CSV_filtr").getOrCreate()

#%% wczytanie danych
df=spark.read.options(header='true',delimiter=',', inferSchema='true')\
    .csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source")

#%% df pofiltrowane po Pythonie
df_Python=df.filter(df.language=="Python")

#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_Python.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_filtr_Python.csv", header=True)
print("Dane zapisane do plików.")
