import pandas as pd
from typing_extensions import Self
from sqlalchemy import create_engine


class DatasetReferenceData:
    def __init__(self) -> None:
        self._data: dict[str, pd.DataFrame] = {}

    def from_xlsx(self, xlsx_filepath: str) -> Self:
        self._data = pd.read_excel(io=xlsx_filepath, sheet_name=None)

    def to_mssql(self, server_name: str, database_name: str, schema_name: str) -> None:
        engine = create_engine(
            "mssql+pyodbc://@"
            + server_name
            + "/"
            + database_name
            + "?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
        )
        try:
            for key, df in self._data.items():
                df.to_sql(
                    name=f"dict_{key}",
                    schema=schema_name,
                    con=engine,
                    if_exists="replace",
                    index=False,
                )
        except:
            print(
                f"Error. Make sure executing on a computer with the database server and windows authentication provides you 'Owner' privileges."
            )
