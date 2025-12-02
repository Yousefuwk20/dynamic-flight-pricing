"""
CSV to Parquet Converter
Converts large CSV files to Parquet format with Snappy compression.

Usage:
    python convert_to_parquet.py 
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


def convert_csv_to_parquet(csv_file: str, parquet_file: str, chunk_size: int = 500_000):
    """Convert CSV file to Parquet format in chunks."""
    total_rows = sum(1 for _ in open(csv_file)) - 1
    chunks_total = (total_rows // chunk_size) + 1
    
    first_chunk = True
    pqwriter = None
    
    for chunk in tqdm(pd.read_csv(csv_file, chunksize=chunk_size), 
                      total=chunks_total, desc="Converting"):
        table = pa.Table.from_pandas(chunk)
        
        if first_chunk:
            pqwriter = pq.ParquetWriter(parquet_file, table.schema, compression='snappy')
            first_chunk = False
        
        pqwriter.write_table(table)
    
    if pqwriter:
        pqwriter.close()
    
    return total_rows


csv_file = "data/itineraries.csv"
parquet_file = "data/itineraries.parquet"
convert_csv_to_parquet(csv_file, parquet_file)