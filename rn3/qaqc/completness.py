from .quality_test import QualityTest
from .dataset import DataSet
from typing import Dict, List
import polars as pl


class Completness(QualityTest):
    def __init__(self, schema_name: str, table_name: str):
        self._table_name = table_name
        self._schema_name = schema_name
        self._filters: Dict[str, object] = dict()
        self._completness_columns: List[str] = list()
        self._empty = None
        self._total = None

    def __str__(self):
        return f"Completness QC '{self.schema_name}.{self.table_name}'."

    def add_filter(self, col_name: str, col_values: object) -> None:
        if col_name not in self.filters:
            self.filters[col_name] = col_values

    def set_filters(self, filters: Dict[str, object]) -> None:
        self._filters = filters

    def set_completness_columns(self, completness_columns: List[str]) -> None:
        self._completness_columns = completness_columns

    @property
    def completness_columns(self) -> List[str]:
        return self._completness_columns

    @property
    def empty(self) -> int:
        return self._empty

    @property
    def total(self) -> int:
        return self._total

    @property
    def filters(self) -> Dict[str, object]:
        return self._filters

    def check(self, dataset: DataSet):
        ds: pl.DataFrame = dataset.get_table(self.table_name)
        filters = []
        for k, v in self._filters.items():
            if isinstance(v, list):
                filters.append(pl.col(k).is_in(v))

            else:
                filters.append(pl.col(k) == v)
        result = ds.filter(filters).select(self.completness_columns)
        self._empty = sum(result.null_count().row(0))
        self._total = sum(result.count().row(0))
