import json
import os
import pytest
from rn3 import DatasetModel
from rn3 import Mysql


@pytest.fixture
def pams_dataset():
    ds = DatasetModel()
    return ds.from_json("tests/data/pam_schema.json")


def test_sql(pams_dataset):
    sql = Mysql()
    sql.create_table(dataset=pams_dataset)
