"""The simulation module"""
import pandas as pd
from ..dataset.dataset_model import DatasetModel


class Xlsx:
    def __init__(self, filename: str, datamodel: DatasetModel = None) -> None:
        """Create a new Import XLSX class

        Parameters
        ----------
        filename: str
            name of simulation
        datamodel: DatasetModel, optional
            database model

        Examples
        --------
        >>> xls = Xlsx(filename="data/excel_file.xlsx", datamodel=data_model_from_json)
        """
        self._filename = filename
        self._datamodel = datamodel

    def read(self, strict=False) -> dict(str, pd.DataFrame):
        """read the xlsx file

        Parameters
        ----------
        strict: bool
            The required fields defined in datamodel are included. Requires datasetmodel
        Returns
        -------
        dictionary of panda dataframes
        """
        if self._datamodel == None:
            data = pd.read_excel(io=self._filename, sheet_name=None)
        else:
            data = {}
            for table_name in self._datamodel.table_names:
                table = self._datamodel.get_table(table_name)
                if not table:
                    raise ValueError(
                        f"Error. Could not find table {table_name} in excel file."
                    )
                data[table_name] = pd.read_excel(
                    io=self._filename,
                    parse_dates=table.date_columns,
                    dtype=table.non_date_fields,
                    sheet_name=table_name,
                )
