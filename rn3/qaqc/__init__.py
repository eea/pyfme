"""FME Python Helper Functions"""
from .quality_test import QualityTest
from .dataset import DataSet
from .completness import Completness
from .read_sql import SQL_Helper

__all__ = ["QualityTest", "DataSet", "Completness"]


def Read_SQL(servername: str, database: str, schema: str) -> DataSet:
    sql_helper = SQL_Helper()
    connection = sql_helper.make_connection(servername=servername, database=database)
    dataset_data = sql_helper.read_schema_polars(schema=schema, connection=connection)
    dataset = DataSet()
    dataset._set_dataset(dataset_data)
    return dataset
