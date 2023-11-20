import json
from datetime import datetime
import numpy as np


class Table:
    def __init__(self, table_json: json) -> None:
        """Create a new Table class

        Args:
            table_json (json): json object of the table.

        """
        self._schema = table_json["recordSchema"]["fieldSchema"]
        self._name = table_json["nameTableSchema"]
        self._column_names_and_type = self._get_column_names_and_type()

    @property
    def name(self) -> str:
        return self._name

    @property
    def columns(self) -> dict[str, object]:
        return self._column_names_and_type

    @property
    def required(self) -> list[str]:
        return [s["name"] for s in self._schema if s["required"]]

    def _get_column_names_and_type(self) -> dict[str, object]:
        names_and_type = {}
        for s in self._schema:
            name = s.get("name")
            v = s.get("type")
            if v == "LINK" or v == "TEXT" or v == "CODELIST":
                v = str
            elif v == "NUMBER_DECIMAL":
                v = np.float64
            elif v == "NUMBER_INTEGER":
                v = "Int64"
            elif v == "DATE":
                v = datetime
            names_and_type[name] = v
        return names_and_type
