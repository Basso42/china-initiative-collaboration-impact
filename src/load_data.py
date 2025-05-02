import polars as pl

def read_parquet_as_dicts(parquet_file: str) -> list:
    """
    Load the works from a Parquet file and return as a list of dictionaries.
    """
    # Read parquet using Polars
    df = pl.read_parquet(parquet_file)
    
    # Convert to list of dicts
    works = df.to_dicts()
    
    return works
