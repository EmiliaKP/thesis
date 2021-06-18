#%% powołanie sesji Apache Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CSV_join_broadcast").getOrCreate()

#%% wczytanie danych
from pyspark.sql.functions import broadcast

df_maly=spark.read.options(header='true',delimiter=',', inferSchema='true')\
    .csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source_broadcast")

df_wielki=spark.read.options(header='true',delimiter=',', inferSchema='true')\
    .csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source")
    
#%% połączenie tabel względem kolumny comment_id
df_join=df_wielki.join(broadcast(df_maly), df_wielki.comment_id==df_maly.comment_id_broad ,'left')

#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_join.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_join.csv", header=True)
print("Dane zapisane do plików.")
