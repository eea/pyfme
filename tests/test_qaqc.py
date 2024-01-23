import pytest
import os
import json

# from rn3 import DataSet
from typing import Dict
from polars import DataFrame
from rn3 import qaqc
from rn3.qaqc import DataSet
from rn3.qaqc import Completness


@pytest.fixture
def annexXXVI_dataset() -> DataSet:
    csv_files = {
        "Table_5": "tests/data/annexXVI.Table_5.csv",
        "Table_1": "tests/data/annexXVI.Table_1.csv",
    }
    return DataSet().from_csv_files(csv_files)


def test_read_sql():
    ds = qaqc.Read_SQL("onager", "NECPR", "annexXVI")
    tables = ds.tables

    df: DataFrame = ds.get_table("Table_5")
    df.write_csv("tests/data/annexXVI.Table_5.csv")

    print(tables)


def test_apply_completness(annexXXVI_dataset):
    c1 = Completness("annexXVI", "Table_1")
    c1.add_filter("Year", 2020)
    c1.add_filter("Sector", "Electricity")
    c1.set_completness_columns(
        [
            "Guarantees_of_origin_cancelled",
            "Resulting_annual_national_RES_consumption_GWh",
        ]
    )
    annexXXVI_dataset.apply_check(c1)
    assert c1.total == 128
    assert c1.empty == 12
