import importlib.resources as pkg_resources
import pandas as pd
from athena_federation_testing import resources
from athena_federation_testing.util import config_helper
import json, re

DATA_TABLES = ["customer", "call_center", "catalog_sales", "date_dim", "item"]
# DATA_TABLES = ["item"]
# DATA_TABLES = ["call_center"]
# DATA_TABLES = ["customer"]
# DATA_TABLES = ["date_dim"]
# DATA_TABLES = ["catalog_sales"]
# DATA_TABLES = ["call_center", "catalog_sales", "date_dim", "item"]

TEST_SELECT_TABLES = ["customer", "call_center", "catalog_sales"]


def load_tpcds_data_parquet_from_resources(table_name: str) -> pd.DataFrame:
    """
    Load all .parquet files from a resource subfolder and return as a single DataFrame.
    """
    df_list = []
    resource_path = pkg_resources.files(resources).joinpath(f'tpcds_data/{config_helper.get_tpcds_scale_factor()}/{table_name}' )

    with pkg_resources.as_file(resource_path) as path:
        if not path.exists() or not path.is_dir():
            raise FileNotFoundError(f"Resource folder '{table_name}' not found at: {path}")

        for file in path.glob("*.parquet"):
            print(f"Reading {file.name}")
            df_list.append(pd.read_parquet(file))

    if df_list:
        return pd.concat(df_list, ignore_index=True)
    else:
        print("No Parquet files found.")
        return pd.DataFrame()  # Return empty DataFrame if none found


def get_tpcds_data_config(table_name: str) -> dict:
    """
    Load all .parquet files from a resource subfolder and return as a single DataFrame.
    """
    resource_path = pkg_resources.files(resources).joinpath(f'tpcds_data/schema/{table_name}.json')

    with pkg_resources.as_file(resource_path) as config_path:
        with open(config_path) as config_file:
            config_data = json.load(config_file)

    return config_data

def get_tpcds_data_primary_key(table_name: str) -> str:
    return get_tpcds_data_config(_get_table_name_with_no_prefix(table_name))['table_setup']['primary_key']

def get_tpcds_data_primary_key_type(table_name: str) -> str:
    return get_tpcds_data_config(_get_table_name_with_no_prefix(table_name))['table_setup']['primary_type']

def get_tpcds_data_sort_key(table_name: str) -> str:
    return get_tpcds_data_config(_get_table_name_with_no_prefix(table_name))['table_setup']['sort_key']

def get_tpcds_data_sort_key_type(table_name: str) -> str:
    return get_tpcds_data_config(_get_table_name_with_no_prefix(table_name))['table_setup']['sort_key_type']

def get_tpcds_data_primary_key_predicate(table_name: str) -> str:
    return get_tpcds_data_config(_get_table_name_with_no_prefix(table_name))['predicate']['primary_key_predicate']

def get_tpcds_data_sort_key_predicate(table_name: str) -> str:
    return get_tpcds_data_config(_get_table_name_with_no_prefix(table_name))['predicate']['sort_key_predicate']

def get_tpcds_data_complex_predicate(table_name: str) -> str:
    return get_tpcds_data_config(_get_table_name_with_no_prefix(table_name))['predicate']['complex_key_predicate']

def _get_table_name_with_no_prefix(table_name: str) -> str:
    return re.sub(r'^tpcds\d+_', '', table_name)

def get_data_tables():
    return DATA_TABLES

def get_simple_select_tables():
    return TEST_SELECT_TABLES