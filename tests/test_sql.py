import json
import os
import pytest
from pyfme import DatasetModel
from pyfme import Mysql


@pytest.fixture
def pams_dataset():
    f = os.path.join(os.getcwd(), r"tests\data\pam_schema.json")
    ds = DatasetModel()
    return ds.from_json(json_filepath=f)


def test_p(pams_dataset):
    sql = Mysql()
    sql.create_table(dataset=pams_dataset)
