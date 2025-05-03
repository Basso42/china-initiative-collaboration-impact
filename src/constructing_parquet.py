import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
from tqdm import tqdm
from pathlib import Path
from pyalex import Works, Institutions, Topics, Funders
import pyalex

from collections import Counter, defaultdict
from itertools import chain




data_path = (Path.cwd().parent / "data").resolve()

pyalex.config.email = "john.doe@gmail.com"

 #Define the filter criteria for works published in the USA
filter_criteria = {
    "from_publication_date": "2016-01-01",  # Start date
    "to_publication_date": "2020-03-15",    # End date
    "institutions": {"country_code": "us"},  # Filter for U.S.-based institutions
    "primary_location": {"source": {"type": "journal"}},  # Filter for journal articles
    "primary_topic": {"subfield": {"id": 2740}},  # Filter for Pulmonary and Respiratory Medicine
}


# Fetch the works with pagination
query = Works().filter(**filter_criteria)
all_works_16_20 = list(chain.from_iterable(query.paginate(per_page=200, n_max=None)))  # Adjust per_page as needed, n_max=None for all papers (heavy)


def flatten_work(work):
    flat = {}
    
    # Simple fields
    flat['id'] = work.get('id')
    flat['doi'] = work.get('doi')
    flat['title'] = work.get('title')
    flat['display_name'] = work.get('display_name')
    flat['publication_year'] = work.get('publication_year')
    flat['publication_date'] = work.get('publication_date')
    flat['language'] = work.get('language')
    flat['type'] = work.get('type')
    flat['type_crossref'] = work.get('type_crossref')
    flat['cited_by_count'] = work.get('cited_by_count')
    flat['fwci'] = work.get('fwci')
    flat['has_fulltext'] = work.get('has_fulltext')
    flat['is_retracted'] = work.get('is_retracted')
    flat['is_paratext'] = work.get('is_paratext')

    # Serialize nested fields
    flat['authorships'] = json.dumps(work.get('authorships', []))
    flat['primary_location'] = json.dumps(work.get('primary_location', {}))
    flat['open_access'] = json.dumps(work.get('open_access', {}))
    flat['topics'] = json.dumps(work.get('topics', []))
    flat['keywords'] = json.dumps(work.get('keywords', []))
    flat['concepts'] = json.dumps(work.get('concepts', []))
    flat['grants'] = json.dumps(work.get('grants', []))
    flat['mesh'] = json.dumps(work.get('mesh', []))
    flat['institutions_distinct_count'] = work.get('institutions_distinct_count')
    flat['countries_distinct_count'] = work.get('countries_distinct_count')
    
    return flat


# Apply the flattening
flattened_works = [flatten_work(work) for work in all_works_16_20]

# Create DataFrame
df_works_flat = pd.DataFrame(flattened_works)


# Define output path
output_file = data_path / "all_works_16_20_flat.parquet"

# Save
df_works_flat.to_parquet(output_file, engine='pyarrow', index=False)

print(f"Saved flattened works to {output_file}")


print("✅ Done: all_works_16_20.parquet saved successfully.")
