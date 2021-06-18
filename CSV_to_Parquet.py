import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

csv_file = "C:\\EMILIA\\test\\ghtorrent-2019-01-07.csv"
parquet_file = "C:\\EMILIA\\test\\ghtorrent-2019-01-07.parquet"
chunksize = 100_000

csv_stream = pd.read_csv(csv_file, sep=',', chunksize=chunksize, low_memory=False)

for i, chunk in enumerate(csv_stream):
    print("Chunk", i)
    if i == 0:
        # odgadnij schemat danych na podstawie pierwszego kawałka danych
        parquet_schema = pa.Table.from_pandas(df=chunk).schema
        # otwórz plik Parquet, aby wczytać dane
        parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
    # zapisz dane z kawałka CSV do pliku Parquet
    table = pa.Table.from_pandas(chunk, schema=parquet_schema)
    parquet_writer.write_table(table)

parquet_writer.close()

print("Zrobione")
