"""FME Python Helper Functions"""
from .quality_test import QualityTest
from .dataset import DataSet
from .completeness import Completeness
from .comparator import Comparator
from .read_sql import SQL_Helper
import polars as pl
from typing import Dict
from typing_extensions import Optional

__all__ = ["QualityTest", "DataSet", "Completeness", "Comparator"]


def from_sql(servername: str, database: str, schema: str) -> DataSet:
    sql_helper = SQL_Helper()
    connection = sql_helper.make_connection(servername=servername, database=database)
    dataset_data = sql_helper.read_schema_polars(schema=schema, connection=connection)
    dataset = DataSet()
    dataset._set_dataset(dataset_data)
    return dataset


def from_csv_files(
    csv_files: Dict[str, str], hist_csv: Optional[str] = None
) -> DataSet:
    data: Dict[str, pl.DataFrame] = dict()
    dataset = DataSet()
    if hist_csv:
        df_hist = pl.read_csv(hist_csv)
    for table, csv_file in csv_files.items():
        df = pl.read_csv(csv_file)
        if hist_csv:
            df = dataset._join_historical_release(df, df_hist)

        data[table] = df
    dataset._set_dataset(data)
    return dataset
