import pytest
from polars import DataFrame
import polars as pl
import pandas as pd
from rn3 import qaqc
from rn3.qaqc import Completeness, Comparator, DataSet
from rn3.qaqc.completeness_set import CompletenessSet


@pytest.fixture
def annexXXVI_dataset() -> DataSet:
    csv_files = {
        "Table_5": "tests/data/annexXVI.Table_5.csv",
        "Table_1": "tests/data/annexXVI.Table_1.csv",
    }

    hist_csv = "tests/data/historical_releases.csv"
    ds = qaqc.from_csv_files(csv_files, hist_csv=hist_csv)
    return ds


@pytest.fixture
def annexXXVI_all_dataset() -> DataSet:
    xlsx_file = "tests/data/annexXVI.xlsx"
    ds = qaqc.from_xlsx(xlsx_file)
    return ds


@pytest.fixture
def pk_found() -> DataFrame:
    return pl.read_csv("tests/data/pk_found.csv", separator=";")


@pytest.fixture
def completeness_set() -> CompletenessSet:
    comp_set = CompletenessSet()
    comp_set.from_csv("tests/data/questions.csv")
    return comp_set


# @pytest.mark.skip(reason="No access to database")
def test_read_sql():
    ds = qaqc.from_sql("onager", "NECPR", "annexXVI")
    tables = [
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


@pytest.mark.skip(reason="No access to database")
def test_write_sql_to_xlsx():
    ds = qaqc.from_sql("onager", "NECPR", "annexXVI")
    tables = [
        "Table_1",
        "Table_1_Measures",
        "Table_2",
        "Table_3",
        "Table_4",
        "Table_5",
        "Table_6",
        "Table_7",
        "Table_8",
    ]
    with pd.ExcelWriter(
        "tests/data/annexXVI.xlsx", mode="w", engine="openpyxl"
    ) as writer:
        for table_name in tables:
            print(table_name)
            data = ds.get_table(table_name).to_pandas()
            data.to_excel(writer, sheet_name=table_name, index=False)


def test_apply_completeness_single_filter(annexXXVI_dataset):
    c1 = Completeness("annexXVI", "Table_1")
    c1.add_filter("Year", "2020")
    c1.add_filter("Sector", "Electricity")
    c1.completeness_columns = [
        "Guarantees_of_origin_cancelled",
        "Resulting_annual_national_RES_consumption_GWh",
    ]
    annexXXVI_dataset.apply_check(c1)
    assert c1.total == 128
    assert c1.empty == 12


def test_apply_completeness_list_filter(annexXXVI_dataset):
    c1 = Completeness("annexXVI", "Table_1")
    c1.add_filter("Year", [2020, 2021])
    c1.add_filter("Sector", "Electricity")
    c1.completeness_columns = [
        "Guarantees_of_origin_cancelled",
        "Resulting_annual_national_RES_consumption_GWh",
    ]
    annexXXVI_dataset.apply_check(c1)
    assert c1.total == 260
    assert c1.empty == 20


def test_apply_comparator(annexXXVI_dataset, pk_found):
    c1 = Comparator("annexXVI", "Table_1", "FR", 2)
    c1.set_pks(pks=[["Sector", "Year"]])
    annexXXVI_dataset.apply_check(c1)

    c1.pivot_results(pks=[["Sector", "Year"]])
    assert c1.results.width == 11
    assert c1.results.height == 12


def test_apply_set_completeness_checks(annexXXVI_all_dataset, completeness_set):
    completeness_set.apply_checks(annexXXVI_all_dataset)
    assert True
