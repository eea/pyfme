from .quality_test import QualityTest
import polars as pl
from typing import Dict, List
from typing_extensions import Optional, Self


class DataSet:
    def __init__(self):
        self._dataset: Dict[str, pl.DataFrame] = dict()
        self._historical: pl.DataFrame = pl.DataFrame()

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

    def from_csv_files(
        self, csv_files: Dict[str, str], hist_csv: Optional[str] = None
    ) -> Self:
        self._dataset: Dict[str, pl.DataFrame] = dict()
        if hist_csv:
            df_hist = pl.read_csv(hist_csv)
        for table, csv_file in csv_files.items():
            df = pl.read_csv(csv_file)
            if hist_csv:
                df = self._join_historical_release(df, df_hist)
            self._dataset[table] = df
        return self

    # def historical_from_csv_files(self, csv_file) -> Self:
    #     self._historical = pl.read_csv(csv_file)
    #     return self

    # def _set_historical(self, df_hist: pl.DataFrame):
    #     self._historical = df_hist

    def _set_dataset(self, dataset: Dict[str, pl.DataFrame]) -> Self:
        self._dataset = dataset
        return self

    @property
    def tables(self) -> List[str]:
        return list(self._dataset.keys())

    # @property
    # def historical(self) -> pl.DataFrame:
    #     return self._historical

    # def last_k_ReportNet3HistoricalReleaseId(
    #     self, country_code: str, rn3_dataflow_id: int, k: int
    # ) -> List[int]:
    #     latest = (
    #         self._historical.filter(pl.col("countryCode") == country_code)
    #         .select(["releaseDate", "Id"])
    #         .sort("releaseDate")
    #         .bottom_k(k, by="releaseDate")
    #         .select("Id")
    #         .to_series()
    #         .to_list()
    #     )
    #     return latest

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
