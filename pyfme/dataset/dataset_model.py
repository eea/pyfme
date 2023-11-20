import json
import requests
from typing import Optional
from .table import Table


class DatasetModel:
    def __init__(self, base_url: str = "https://sandbox.reportnet.europa.eu") -> None:
        """Create a new DatasetModel class

        Parameters
        ----------
        base_url: str
            base url (default to sandbox)

        Examples
        --------
        >>> sim = DatasetModel(base_url="https://sandbox.reportnet.europa.eu")
        """
        self._base_url = base_url.removesuffix(r"/")

    def get(self, dataset_id: str, api_key: str) -> None:
        headers = {"Authorization": api_key}
        endpoint = self._base_url + r"/dataschema/v1/datasetId/" + dataset_id
        request = requests.get(endpoint, headers=headers)
        if not request.ok:
            raise Exception(
                f"Status Code: {request.status_code}. Could not retrieve schema with GET: {endpoint}."
            )
        schema = request.json()
        self._read_schema(schema)

    def _read_schema(self, schema: json):
        self._tables = []
        for table in schema["tableSchemas"]:
            self._tables.append(Table(table))

    @property
    def table_names(self) -> list[str]:
        """Returns a list of table names.

        Returns:
            A list of table names extracted from the input tables.

        """
        return [table.name for table in self._tables]

    def get_table(self, table_name: str) -> Optional[Table]:
        return next((t for t in self._tables if t.name == table_name), None)
