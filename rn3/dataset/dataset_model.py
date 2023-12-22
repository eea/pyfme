import json
import requests
from typing import Optional, Union
from typing_extensions import Self
from .table import Table


class DatasetModel:
    def __init__(self) -> None:
        self._tables = []
        self._name = ""

    def __repr__(self) -> str:
        return "Dataset (" + self.name + ")"

    @property
    def name(self) -> str:
        return self._name

    def from_json(self, json_filepath: str) -> Self:
        json_data = json.load(open(json_filepath))
        self._tables = []
        self._name = json_data["nameDatasetSchema"]
        for table in json_data["tableSchemas"]:
            self._tables.append(Table(table))
        return self

    def from_url(
        self,
        dataset_id: Union[int, str],
        api_key: str,
        base_url: str = "https://api.reportnet.europa.eu",
    ) -> Self:
        headers = {"Authorization": api_key}
        endpoint = base_url + r"/dataschema/v1/datasetId/" + str(dataset_id)
        request = requests.get(endpoint, headers=headers)
        if not request.ok:
            raise Exception(
                f"Status Code: {request.status_code}. Could not retrieve schema with GET: {endpoint}."
            )
        json_data = request.json()
        self._tables = []
        self._name = json_data["nameDatasetSchema"]
        for table in json_data["tableSchemas"]:
            self._tables.append(Table(table))
        return self

    def sql_cmd(self, database_name=None, schema_name=None) -> str:
        sql_cmd = ""
        for table in self.tables:
            sql_cmd += "\n"
            tbl_cmd = table.sql_create_cmd
            if database_name is not None:
                tbl_cmd = tbl_cmd.replace("DATABASE_NAME", database_name)
            if schema_name is not None:
                tbl_cmd = tbl_cmd.replace("SCHEMA_NAME", schema_name)
            sql_cmd += tbl_cmd + "\n"
        return sql_cmd

    @property
    def table_names(self) -> list[str]:
        """Returns a list of table names.

        Returns:
            A list of table names extracted from the input tables.

        """
        return [table.name for table in self._tables]

    @property
    def tables(self) -> list[Table]:
        """Returns a list table objects.

        Returns:
            A list of table objects.
        """
        return self._tables

    def remove_table(self, table_name: str) -> Self:
        table = self.get_table(table_name=table_name)
        if table is None:
            raise ValueError(f"Cannot fine table {table_name} in dataset")
        self._tables.remove(table)
        return self

    def get_table(self, table_name: str) -> Optional[Table]:
        return next((t for t in self._tables if t.name == table_name), None)
