from .quality_test import QualityTest
import polars as pl
from typing import Dict, List
from typing_extensions import Self


class DataSet:
    def __init__(self):
        self._dataset: Dict[str, pl.DataFrame] = dict()

    def __repr__(self) -> str:
        return f"Dataset ({self.tables})"

    def from_polars(self, dataset: Dict[str, pl.DataFrame]) -> Self:
        self._dataset = dataset
        return self

    def from_pandas(self, dataset: Dict[str, pl.DataFrame]) -> Self:
        self._dataset = dataset
        self._dataset: Dict[str, pl.DataFrame] = dict()
        for table, df in dataset.items():
            self._dataset[table] = pl.from_pandas(df)
        return self

    def from_csv_files(self, csv_files: Dict[str, str]) -> Self:
        self._dataset: Dict[str, pl.DataFrame] = dict()
        for table, csv_file in csv_files.items():
            self._dataset[table] = pl.read_csv(csv_file)
        return self

    def _set_dataset(self, dataset: Dict[str, pl.DataFrame]) -> Self:
        self._dataset = dataset
        return self

    @property
    def tables(self) -> List[str]:
        return list(self._dataset.keys())

    def get_table(self, table_name: str) -> pl.DataFrame:
        if table_name in self._dataset:
            return self._dataset[table_name]

    # def from_sql(self, servername, database, schema) -> Self:
    #     sql_helper = Read_SQL()
    #     connection = sql_helper.make_connection(
    #         servername=servername, database=database
    #     )
    #     self._dataset = sql_helper.read_schema_polars(
    #         schema=schema, connection=connection
    #     )
    #     return self

    def apply_check(self, quality_test: QualityTest):
        quality_test.check(dataset=self)
