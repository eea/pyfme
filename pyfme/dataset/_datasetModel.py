import requests


class DatasetModel:
    table_list: list[str]

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
        self.table_list = self._get_table_names(schema["tableSchemas"])

    def _get_table_names(tables: list) -> list:
        """Returns a list of table names from a given list of tables.

        Args:
            tables (list): A list of tables where each table is a dictionary with a "nameTableSchema" key.

        Returns:
            A list of table names extracted from the input tables.

        """
        return [table["nameTableSchema"] for table in tables]
