from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CSV_grup_multi").getOrCreate()

#%% wczytanie danych
from pyspark.sql.functions import col
# wczytanie wszystkich plików CSV w danym folderze
df=spark.read.options(header='true',delimiter=',', inferSchema='true')\
    .csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/source")\
    .select('language','repo','author_login').filter(col('comment_id')>270000000)

#%% stworzenie kilku df grupujących
df_repo_lang=df.groupBy(df.repo, df.language).count()
df_lang=df.groupBy(df.language).count()
df_repo=df.groupBy(df.repo).count()
df_author=df.groupBy(df.author_login).count()

#%% zapis do pliku
print("Kalkulacja i wczytywanie do plików...")
df_repo_lang.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_grup_repo_lang.csv", header=True)
df_lang.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_grup_lang.csv", header=True)
df_repo.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_grup_repo.csv", header=True)
df_author.write.mode("overwrite").csv("/home/ubuntu/spark-3.0.1-bin-hadoop2.7/outputs/CSV_grup_author.csv", header=True)

print("Dane zapisane do plików.")
