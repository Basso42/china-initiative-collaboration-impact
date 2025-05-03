import polars as pl
import json

json_columns = ['ids', 'primary_location', 'indexed_in', 'open_access', 'authorships', 'institution_assertions', 'corresponding_author_ids', 'corresponding_institution_ids', 'apc_list', 'citation_normalized_percentile', 'cited_by_percentile_year', 'biblio', 'primary_topic', 'topics', 'keywords', 'concepts', 'mesh', 'locations', 'best_oa_location', 'sustainable_development_goals', 'grants', 'datasets', 'versions', 'referenced_works', 'related_works', 'counts_by_year', 'abstract_inverted_index', 'apc_paid']  #default


def read_parquet_as_dicts(parquet_file: str, json_columns=None) -> list:
    """
    Safe loading of works from a Parquet file, handling JSON fields.
    """
    try:
        if json_columns is None:
            json_columns = []

        df = pl.read_parquet(parquet_file)

        for col in json_columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).apply(lambda x: json.loads(x) if isinstance(x, str) else x)
                )

        return df.to_dicts()

    except FileNotFoundError:
        raise FileNotFoundError(f"Parquet file not found: {parquet_file}")
    except Exception as e:
        raise RuntimeError(f"Error reading parquet file: {e}")

