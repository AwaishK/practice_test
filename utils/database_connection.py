"""This file acts as wrapper to communicate to database
"""
from io import StringIO
import pandas as pd
import psycopg2
from utils.config_parser import configuration_parser


class SetupDB:
    DATABASE = "trading_data"
    SCHEMA = "trading_data"

    def __init__(self) -> None:
        config = configuration_parser()
        self.database_config = dict(config["DATABASE"])
        self.database_config["dbname"] = self.DATABASE

        self.database_config = {k.lower(): v for k, v in self.database_config.items()}
        self.ensure_schema_exists()
    
    def ensure_schema_exists(self):
        """This will ensure schema trading_data exists
        """
        self.query(f'CREATE SCHEMA IF NOT EXISTS {self.SCHEMA}')

    def connect(self, readonly: bool = False):
        """
        Make a connection to database
        :param readonly (bool) used as a paramter to database session to inform that if its a readonly session
        """
        conn = psycopg2.connect(**self.database_config)
        conn.set_session(readonly=readonly, autocommit=True)
        return conn
    
    def query(self, query: str) -> None:
        """
        Executes the query on database
        :param query (str) sql query to execute
        """
        try:
            conn = self.connect()
            with conn.cursor() as cursor:
                cursor.execute(f'SET search_path TO {self.SCHEMA}, public')
                cursor.execute(query)
            conn.commit()
        finally:
            conn.close()

    def recieve(self, query: str) -> pd.DataFrame:
        """
        Recieves the data from postgres 
        :param query (str) sql query to execute

        Returns the dataframe
        """
        conn = self.connect(readonly=True)
        df = pd.read_sql_query(query, con=conn)
        return df
    
    def load_data_from_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Loads the data from dataframe to postgres
        :param query (str) sql query to execute
        """
        output = StringIO()
        df.to_csv(output, sep="|", header=False, encoding="utf8", index=False)
        columns = list(df.columns)
        output.seek(0)

        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO {self.SCHEMA}, public')
            cur.copy_from(output, table_name, sep="|", null="", size=100_000, columns=columns)
        conn.commit()

