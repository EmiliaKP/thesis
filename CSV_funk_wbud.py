from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CSV_funk_wbud").getOrCreate()

#%% wczytanie danych

# wczytanie wszystkich plików CSV w danym folderze
df=spark.read.options(header='true',delimiter=',', inferSchema='true')\
    .csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source")


#%% przykład z wbudowaną funkcją
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as f

df_len=df.withColumn('comment_lenght',f.length(df.comment))
    
    
#%% zapis do pliku
print("Kalkulacja i wczytywanie do pliku...")
df_len.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_funk_wbud.csv", header=True)
print("Dane zapisane do pliku.")


