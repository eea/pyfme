import json
import os
import pandas as pd
import pytest
from rn3 import Xlsx
from rn3 import DatasetModel
from pathlib import Path


@pytest.fixture
def nitrate_dataset():
    f = os.path.join(os.getcwd(), "tests", "data", "nitrate_schema.json")
    f = Path(f)
    ds = DatasetModel()
    return ds.from_json(json_filepath=f)


@pytest.fixture
def filename_xlsx():
    file_path = os.path.join(os.getcwd(), "tests", "data", "fake_italy_reporting_tiny.xlsx")
    return Path(file_path)


@pytest.fixture
def zip_file(tmpdir_factory, filename_xlsx):
    outfile = tmpdir_factory.mktemp("data").join("xlsx.zip")
    xlsx = Xlsx()
    xlsx.from_xlsx(filename=filename_xlsx)
    xlsx.to_csv_zip(outfile)
    return outfile


def test_read_xlsx_without_schema(filename_xlsx):
    xlsx = Xlsx()
    xlsx.from_xlsx(filename=filename_xlsx)
    assert xlsx is not None


def test_read_pandas(filename_xlsx):
    df = pd.read_excel(filename_xlsx)
    xlsx = Xlsx()
    xlsx.from_pandas(dataset=df)
    assert xlsx is not None


def test_read_xlsx_with_schema(nitrate_dataset, filename_xlsx):
    xlsx = Xlsx()
    xlsx.from_xlsx(filename=filename_xlsx, datamodel=nitrate_dataset)
    assert xlsx is not None


def test_write_xlsx_to_zip(zip_file):
    assert os.path.isfile(zip_file)
