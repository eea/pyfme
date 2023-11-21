import json
import requests
from typing import Optional, Self
from .table import Table


class DatasetModel:
    def __init__(self) -> None:
        self._tables = []

    def from_json(self, json_filepath: str) -> Self:
        json_data = json.load(open(json_filepath))
        self._tables = []
        for table in json_data["tableSchemas"]:
            self._tables.append(Table(table))
        return self

    def from_url(self, base_url: str, dataset_id: str, api_key: str) -> Self:
        headers = {"Authorization": api_key}
        endpoint = base_url + r"/dataschema/v1/datasetId/" + dataset_id
        request = requests.get(endpoint, headers=headers)
        if not request.ok:
            raise Exception(
                f"Status Code: {request.status_code}. Could not retrieve schema with GET: {endpoint}."
            )
        json_data = request.json()
        self._tables = []
        for table in json_data["tableSchemas"]:
            self._tables.append(Table(table))
        return self

    @property
    def table_names(self) -> list[str]:
        """Returns a list of table names.

        Returns:
            A list of table names extracted from the input tables.

        """
        return [table.name for table in self._tables]

    def remove_table(self, table_name: str) -> Self:
        table = self.get_table(table_name=table_name)
        if table is None:
            raise ValueError(f"Cannot fine table {table_name} in dataset")
        self._tables.remove(table)
        return self

    def get_table(self, table_name: str) -> Optional[Table]:
        return next((t for t in self._tables if t.name == table_name), None)
