import json
import os
import pytest
from pyfme import Xlsx
from pyfme import DatasetModel


@pytest.fixture
def nitrate_schema():
    f = os.path.join(os.getcwd(), "tests/data/nitrate_schema.json")
    j = json.load(open(f))
    dm = DatasetModel()
    dm._read_schema(j)

    return dm


@pytest.fixture
def filename_xlsx():
    return os.path.join(os.getcwd(), "tests/data/fake_italy_reporting_tiny.xlsx")


def test_read_xlsx_without_schema(filename_xlsx):
    xlsx = Xlsx(filename=filename_xlsx)
    data = xlsx.read()
    assert xlsx is not None


def test_read_xlsx_with_schema(nitrate_schema, filename_xlsx):
    xlsx = Xlsx(filename=filename_xlsx, datamodel=nitrate_schema)
    data = xlsx.read()
    assert xlsx is not None
