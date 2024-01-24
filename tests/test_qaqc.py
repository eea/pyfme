import pytest
import os
import json

# from rn3 import DataSet
from typing import Dict
from polars import DataFrame
import polars as pl
from rn3 import qaqc
from rn3.qaqc import Completness, Comparator, DataSet


@pytest.fixture
def annexXXVI_dataset() -> DataSet:
    csv_files = {
        "Table_5": "tests/data/annexXVI.Table_5.csv",
        "Table_1": "tests/data/annexXVI.Table_1.csv",
    }

    hist_csv = "tests/data/historical_releases.csv"
    ds = DataSet().from_csv_files(csv_files, hist_csv=hist_csv)
    return ds


@pytest.fixture
def questions_df() -> DataFrame:
    return pl.read_csv("tests/data/questions.csv")


@pytest.mark.skip(reason="No access to database")
def test_read_sql():
    ds = qaqc.Read_SQL("onager", "NECPR", "annexXVI")
    tables = [
        "pivoted_tables",
        "Table_1",
        "Table_1_Measures",
        "Table_2",
        "Table_3",
        "Table_4",
        "Table_4_quantitative",
        "Table_4_quantitative...stock_type",
        "Table_5",
        "Table_6",
        "Table_7",
        "Table_8",
    ]

    assert len(ds.tables) == len(tables)


def test_apply_completness_single_filter(annexXXVI_dataset):
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


def test_apply_completness_list_filter(annexXXVI_dataset):
    c1 = Completness("annexXVI", "Table_1")
    c1.add_filter("Year", [2020, 2021])
    c1.add_filter("Sector", "Electricity")
    c1.set_completness_columns(
        [
            "Guarantees_of_origin_cancelled",
            "Resulting_annual_national_RES_consumption_GWh",
        ]
    )
    annexXXVI_dataset.apply_check(c1)
    assert c1.total == 260
    assert c1.empty == 20


def test_apply_comparator(annexXXVI_dataset):
    c1 = Comparator("annexXVI", "Table_1", "FR", 2)
    annexXXVI_dataset.apply_check(c1)
    assert True
