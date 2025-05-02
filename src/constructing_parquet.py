import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
from tqdm import tqdm

pyalex.config.email = "john.doe@gmail.com"

# Define the filter
filter_criteria = {
    "from_publication_date": "2016-01-01",
    "to_publication_date": "2020-03-15",
    "institutions": {"country_code": "us"},
    "primary_location": {"source": {"type": "journal"}},
    "primary_topic": {"subfield": {"id": 2740}},
}

# Flatten complex columns (fixed: no warnings)
def flatten_dataframe(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].map(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df

# Start query
query = Works().filter(**filter_criteria)

parquet_file = 'all_works_16_20.parquet'
writer = None
schema = None

pbar = tqdm()

for page in query.paginate(per_page=200, n_max=None):
    df_page = pd.DataFrame(page)
    df_page = flatten_dataframe(df_page)

    if schema is None:
        table = pa.Table.from_pandas(df_page)
        schema = table.schema
        writer = pq.ParquetWriter(parquet_file, schema)
    else:
        for col in schema.names:
            if col not in df_page.columns:
                df_page[col] = None
        df_page = df_page[schema.names]
        table = pa.Table.from_pandas(df_page, schema=schema)

    writer.write_table(table)
    pbar.update(1)

pbar.close()

if writer:
    writer.close()

print("✅ Done: all_works_16_20.parquet saved successfully.")
