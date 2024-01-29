from sqlalchemy import create_engine, inspect, Connection
from typing import Dict, List
import pandas as pd
import polars as pl


class SQL_Helper:
    def __init__(self):
        a = "do nothing"

    def make_connection(self, servername: str, database: str) -> Connection:
        engine = create_engine(
            "mssql+pyodbc://@"
            + servername
            + "/"
            + database
            + "?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server"
        )
        return engine.connect()

    def read_schema_pandas(
        self, schema: str, connection: Connection
    ) -> Dict[str, pd.DataFrame]:
        inspector = inspect(connection)
        table_names = inspector.get_table_names(schema=schema)

        dict_names = [t[5::] for t in table_names if t[0:5] == "dict_"]
        tables = [t for t in table_names if t[0:5] != "dict_" and not "\\" in t]

        dfs = {}
        dicts = self._read_dicts(schema, dict_names, connection)

        df_historical = self._read_historical_release(connection=connection)
        exclude_list = ["pivoted_tables"]
        for table in tables:
            if table in exclude_list:
                continue
            query = f"SELECT * FROM {schema}.{table}"
            df = pd.read_sql(sql=query, con=connection)

            # TODO: make a seperate match and replace function that also handles
            #  multiple replaces.
            matches = list(set(dict_names) & set(df.columns))
            for match in matches:
                if set(df[match].values).issubset(dicts[match].Id.values):
                    d = dict(zip(dicts[match].Id, dicts[match].Value))
                    df.replace({match: d}, inplace=True)

            df = self._join_historical_release(df, df_historical)
            dfs[table] = df
        return dfs

    # NOTE. Not read directly into polars because sometimes errors. TODO. Explore why
    def read_schema_polars(
        self, schema: str, connection: Connection
    ) -> Dict[str, pl.DataFrame]:
        dfs_pandas = self.read_schema_pandas(schema=schema, connection=connection)
        dfs_polars = {}
        for table, df in dfs_pandas.items():
            dfs_polars[table] = pl.from_pandas(df)
        return dfs_polars

    def _read_historical_release(self, connection: Connection) -> pd.DataFrame:
        query = f"SELECT Id, countryCode, ReportNet3DataflowId, releaseDate, isLatestRelease FROM metadata.ReportNet3HistoricReleases"
        return pd.read_sql(sql=query, con=connection)

    def _read_dicts(
        self, schema: str, dict_names: List[str], connection: Connection
    ) -> Dict[str, pd.DataFrame]:
        dicts = {}
        for d in dict_names:
            query = f"SELECT * FROM {schema}.dict_{d}"
            df = pd.read_sql(sql=query, con=connection)
            dicts[d] = df
        return dicts

    def _join_historical_release(
        self, df_reported: pd.DataFrame, df_historical: pd.DataFrame
    ) -> pd.DataFrame:
        df = df_reported.merge(
            df_historical,
            left_on="ReportNet3HistoricReleaseId",
            right_on="Id",
            how="inner",
        )
        df.drop(["Id_y"], axis=1, inplace=True)
        df.rename(columns={"Id_x": "Id"}, inplace=True)
        return df
