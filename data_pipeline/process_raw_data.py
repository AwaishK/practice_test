"""This file handles the data processing for trading system
"""
import datetime
from typing import Set
import pandas as pd

from trading_system.raw_data import get_raw_data
from utils.database_connection import SetupDB


class LoadRawData:
    TABLE = "raw_data"

    def __init__(self, dt_from: datetime.datetime, dt_to: datetime.datetime) -> None:
        """
        Load raw data to postgres 
        :param dt_from (pd.Timestamp): datetime from which to load the data
        :param dt_to (pd.Timestamp): datetime up to which to load the data
        """

        self.dt_from = pd.Timestamp(dt_from)
        self.dt_to = pd.Timestamp(dt_to)
        self.db = SetupDB()
    
    def ensure_table_exists(self) -> None:
        """
        Ensures table exists in postgres, this table uses indexes to better perform queries
        """
        query = Query.create_table.format(table=self.TABLE)
        self.db.query(query)

    def extract(self) -> pd.DataFrame:
        """
        Extracts the data from trading system
        
        Returns raw data
        """
        df = get_raw_data(
            dt_from=self.dt_from, 
            dt_to=self.dt_to
        )
        return df
    
    def load(self, df: pd.DataFrame) -> None:
        """
        Load data to postgres
        :param df(pd.DataFrame) data to load
        """
        self.db.load_data_from_dataframe(df, table_name=self.TABLE)
    
    def run(self):
        print("processing raw data")
        self.ensure_table_exists()
        df = self.extract()
        self.load(df)


class Query:
    create_table = """
        BEGIN;
        CREATE TABLE IF NOT EXISTS trading_data.{table} (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            trade_id integer not null,
            execution_time timestamp without time zone not null,
            price float not null,
            volume float not null,
            market varchar(2) not null,
            product varchar(11) not null,
            product_duration varchar(5)  not null,
            product_type varchar(5) not null
        );
        CREATE INDEX IF NOT EXISTS  idx_trade_id ON {table}(trade_id);
        CREATE INDEX IF NOT EXISTS  idx_execution_time ON {table}(execution_time);
        CREATE INDEX IF NOT EXISTS  idx_market ON {table}(market);
        CREATE INDEX IF NOT EXISTS  idx_product ON {table}(product);
        CREATE INDEX IF NOT EXISTS  idx_product_duration ON {table}(product_duration);
        CREATE INDEX IF NOT EXISTS  idx_product_type ON {table}(product_type);
        COMMIT;
    """

