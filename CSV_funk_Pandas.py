from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CSV_funk_Pandas").getOrCreate()

#%% wczytanie danych

# wczytanie wszystkich plików CSV w danym folderze
df=spark.read.options(header='true',delimiter=',', inferSchema='true')\
    .csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source")

#%% przykład z UDF
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

import pandas as pd
from pyspark.sql.functions import pandas_udf

from pyspark.sql.types import *
from pyspark.sql.functions import *

@pandas_udf(IntegerType())
def pandas_lenght(comment: pd.Series) -> pd.Series:
    return comment.str.len()

df_Pandas=df.withColumn('comment_lenght',pandas_lenght(df.comment))
    
#%% zapis do pliku
print("Kalkulacja i wczytywanie do pliku...")
df_Pandas.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_funk_Pandas.csv", header=True)
print("Dane zapisane do pliku.")
