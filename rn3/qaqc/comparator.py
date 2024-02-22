from .quality_test import QualityTest
from .dataset import DataSet
from typing import Dict, List
import polars as pl


class Comparator(QualityTest):
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        country_code: str,
        number_of_latest: int = 2,
    ):
        self._table_name = table_name
        self._schema_name = schema_name
        self._country_code = country_code
        self._number_of_latest = number_of_latest
        self._pks: List[List[str]] = list()

    def __str__(self):
        return f"Comparator QC '{self.schema_name}.{self.table_name}'."

    def set_pks(self, pks: List[List[str]]) -> None:
        self._pks = pks

    def check(self, dataset: DataSet):
        ds: pl.DataFrame = dataset.get_table(self.table_name)
        latest_ids = (
            ds.filter(pl.col("countryCode") == self._country_code)
            .select(["releaseDate", "ReportNet3HistoricReleaseId"])
            .unique(subset="releaseDate")
            .sort("releaseDate")
            .bottom_k(self._number_of_latest, by="releaseDate")
            .select("ReportNet3HistoricReleaseId")
            .to_series()
            .to_list()
        )
        results = ds.filter(pl.col("ReportNet3HistoricReleaseId").is_in(latest_ids))
        self._results = results

    def pivot_results(self)-> pl.DataFrame:
        variables = list(
            filter(
                lambda i: i
                not in [
                    "ReportNet3HistoricReleaseId",
                    "countryCode",
                    "ReportNet3DataflowId",
                    "releaseDate",
                    "isLatestRelease",
                    "Id"
                ] + self._pks[0],
                self._results.columns,
            )
        )

        melted = self.results.melt(id_vars=self._pks[0] + ["ReportNet3HistoricReleaseId"], value_vars= variables)
        self._pivot_table =  melted.pivot(values= ["value"], index=self._pks[0]+["variable"], columns=["ReportNet3HistoricReleaseId"], aggregate_function="first")
        return self._pivot_table

    @property
    def pivot_table(self):
        return self._pivot_table

    @property
    def results(self):
        return self._results

    def last_k_ReportNet3HistoricalReleaseId(
        self, country_code: str, k: int
    ) -> List[int]:
        latest = (
            self._historical.filter(pl.col("countryCode") == country_code)
            .select(["releaseDate", "ReportNet3HistoricReleaseId"])
            .unique(subset="releaseDate")
            .sort("releaseDate")
            .bottom_k(k, by="releaseDate")
            .select("Id")
            .to_series()
            .to_list()
        )
        return latest
