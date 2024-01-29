from .completeness import Completeness
from .dataset import DataSet
import pandas as pd
from typing import List


class CompletenessSet:
    def __init__(self):
        self._checks: List[Completeness] = list()

    def __repr__(self) -> str:
        return f"Completeness Set with ({len(self._checks)} checks)"

    def from_csv(self, filepath: str) -> None:
        checks_df = pd.read_csv(filepath)
        checks_df.sort_values(
            by=["table_schema", "table_name"], ascending=[True, True], inplace=True
        )
        self._from_df(checks_df)

    def _from_df(self, df: pd.DataFrame) -> None:
        for index, row in df.iterrows():
            if row["qc_type"].lower() != "completeness":
                raise ValueError("Only Completeness checks")

            complete = Completeness(
                schema_name=row["table_schema"], table_name=row["table_name"]
            )
            complete.code = row["code"]
            complete.text = row["text"]
            complete.sub_type = row["sub_type"]
            for i in range(1, 4):
                try:
                    name = row[f"column_name_filter{i}"].strip("'").strip('"')
                    value = row[f"column_value_filter{i}"].strip("'").strip('"')
                    if None not in (name, value):
                        complete.add_filter(name, value)
                except Exception:
                    pass

            columns_included = [
                c.strip("'").strip('"') for c in row["columns_included"].split(",")
            ]
            complete.completeness_columns = columns_included
            self._checks.append(complete)

    @property
    def checks(self) -> List[Completeness]:
        return self._checks

    def apply_checks(self, dataset: DataSet) -> None:
        for check in self._checks:
            print(f"Check: {check.code} ", end=". ", flush=True)
            check.check(dataset=dataset)
            print(
                f"Empty {check.empty}  /  Total {check.total}.", end=" \n", flush=False
            )
