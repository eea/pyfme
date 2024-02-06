from .quality_test import QualityTest
from .dataset import DataSet
from typing import Dict, List, Union
import polars as pl


class Completeness(QualityTest):
    def __init__(self, schema_name: str, table_name: str):
        self._table_name = table_name
        self._schema_name = schema_name
        self._filters: Dict[str, object] = dict()
        self._completeness_columns: List[str] = list()
        self._code = ""
        self._test = ""
        self._sub_type = ""
        self._empty = None
        self._total = None

    def __str__(self):
        return f"Completeness QC '{self.schema_name}.{self.table_name}'."

    @property
    def code(self) -> str:
        return self._code

    @code.setter
    def code(self, value: str) -> None:
        self._code = value

    @property
    def sub_type(self) -> str:
        return self._sub_type

    @sub_type.setter
    def sub_type(self, value: str) -> None:
        self._sub_type = value

    @property
    def text(self) -> str:
        return self._text

    @text.setter
    def text(self, value: str) -> None:
        self._text = value

    @property
    def completeness_columns(self) -> List[str]:
        return self._completeness_columns

    @completeness_columns.setter
    def completeness_columns(self, completeness_columns: List[str]) -> None:
        self._completeness_columns = completeness_columns

    @property
    def empty(self) -> int:
        return self._empty

    @property
    def total(self) -> int:
        return self._total

    @property
    def filters(self) -> Dict[str, object]:
        return self._filters

    @filters.setter
    def filters(self, filters: Dict[str, object]) -> None:
        self._filters = filters

    def add_filter(self, col_name: str, col_values: object) -> None:
        if col_name not in self.filters:
            self.filters[col_name] = self._filter_to_strings(col_values)

    def check(self, dataset: DataSet):
        ds: pl.DataFrame = dataset.get_table(self.table_name)
        filters = []
        for column_name, v in self._filters.items():
            #match case instensitve
            column_name_correct_case =[c for c in ds.columns if c.lower() == column_name.lower()]
            if len(column_name_correct_case) != 1:
                raise ValueError(f"Completeness Test {self.code} cannot find Column {column_name} in dataset.")
            column_name_correct_case = column_name_correct_case[0]
            if isinstance(v, list):
                filters.append(pl.col(column_name_correct_case).is_in(v))
            else:
                filters.append(pl.col(column_name_correct_case) == v)

        # For case insensitive matching:
        completeness_columns = []
        completeness_lower = [c.lower() for c in self.completeness_columns]
        for c in ds.columns:
            if c.lower() in completeness_lower:
                completeness_columns.append(c)
        if len(completeness_columns) != len(self.completeness_columns):
            raise Warning(
                f"Error in search pattern. The 'Columns_included': [{self._completeness_columns}] not in the table: '{self.table_name}' which has [{ds.columns}]"
            )

        if len(filters) == 0:
            result = ds.select(completeness_columns)
        else:
            result = ds.filter(filters).select(completeness_columns)

        self._empty = sum(result.null_count().row(0))
        self._total = result.shape[0] * result.shape[1]

    def _filter_to_strings(self, col_value: Union[list, str]) -> Union[list, str]:
        if isinstance(col_value, list):
            values = [str(v) for v in col_value]
        else:
            values = col_value.split(",")
            values = [str(v) for v in values]
        if len(values) == 1:
            return values[0]
        return values
