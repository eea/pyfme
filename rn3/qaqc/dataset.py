from .quality_test import QualityTest
from .read_sql import SQL_Helper
import polars as pl
from typing import Dict, List
from typing_extensions import Optional, Self


class DataSet:
    def __init__(self):
        self._dataset: Dict[str, pl.DataFrame] = dict()
        # self._historical: pl.DataFrame = pl.DataFrame()

    def __repr__(self) -> str:
        return f"Dataset ({self.tables})"

    def set_polars(self, dataset: Dict[str, pl.DataFrame]) -> Self:
        self._dataset = dataset
        return self

    def set_pandas(self, dataset: Dict[str, pl.DataFrame]) -> Self:
        self._dataset = dataset
        self._dataset: Dict[str, pl.DataFrame] = dict()
        for table, df in dataset.items():
            self._dataset[table] = pl.from_pandas(df)
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

    def apply_check(self, quality_test: QualityTest):
        quality_test.check(dataset=self)

    def _join_historical_release(
        self, df_reported: pl.DataFrame, df_historical: pl.DataFrame
    ) -> pl.DataFrame:
        df = df_reported.join(
            df_historical,
            left_on="ReportNet3HistoricReleaseId",
            right_on="Id",
            how="inner",
        )
        df = df.drop([""])
        return df
