import pytest
import os
import json
from pyfme import DatasetModel, Table


@pytest.fixture
def nitrate_schema():
    f = os.path.join(os.getcwd(), "tests/data/nitrate_schema.json")
    return json.load(open(f))


def test_read_json_ok(nitrate_schema):
    ds = DatasetModel()
    ds._read_schema(nitrate_schema)
