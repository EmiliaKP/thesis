#%% powołanie sesji Apache Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("PRQT_grup_multi").getOrCreate()

#%% wczytanie danych
from pyspark.sql.functions import col

df=spark.read.parquet("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source_p/ghtorrent-2019-01-07.parquet")\
    .select('language','author_login','actor_login', 'repo')\
    .filter(col('comment_id')>270000000)

#%% stworzenie kilku df grupujących
df_lang=df.groupBy(df.language).count()
df_repo_lang=df.groupBy(df.repo, df.language).count()
df_repo=df.groupBy(df.repo).count()
df_author=df.groupBy(df.author_login).count()

#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_lang.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_grup.csv", header=True)
df_repo_lang.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_grup.csv", header=True)
df_repo.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_grup.csv", header=True)
df_author.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/PRQT_grup.csv", header=True)
print("Dane zapisane do plików.")

