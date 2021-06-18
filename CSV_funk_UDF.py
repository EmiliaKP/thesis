from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CSV_funk_UDF").getOrCreate()

#%% wczytanie danych

# wczytanie wszystkich plików CSV w danym folderze
df=spark.read.options(header='true',delimiter=',', inferSchema='true')\
    .csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source")

#%% przykład z UDF
from pyspark.sql.types import *
from pyspark.sql.functions import *

@udf(IntegerType())
def lenght_function(comment):
    return len(comment)
    
df_UDF=df.withColumn('comment_lenght',lenght_function(df.comment))
    
#%% zapis do pliku
print("Kalkulacja i wczytywanie do pliku...")
df_UDF.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_funk_UDF.csv", header=True)
print("Dane zapisane do pliku.")

