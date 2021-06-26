
"""This file adds the views to handle user requests
"""
from fastapi import HTTPException

import datetime
import json
from pytimeparse.timeparse import timeparse
from typing import Optional, Union

from data_pipeline.process_raw_data import Query
from utils.database_connection import SetupDB


async def home(
    col: str, 
    agg: str, 
    groupby: Optional[str] = None, 
    product_duration: Optional[str] = None,
    product_type: Optional[str] = None,
    product: Optional[str] = None,
    market: Optional[str] = None,
    trade_id: Optional[int] = None,
    dt_from: Optional[str] = None,
    dt_to: Optional[str] = None,
    freq: Optional[str] = None,
    n_largest: Optional[str] = None,
    n_smallest: Optional[str] = None,
) -> Union[HTTPException, str]:   
    """
    This takes the parameters from user and returns response
    :param col (str) Name of column (price or volume)
    :param agg (str) Name of aggregate function (min, max, avg, vwap)
    :param groupby (Optional, str) Name of column to be used in group by
    :param product_duration (Optional, str) value of product duration 
    :param product_type (Optional, str) value of product type
    :param product (Optional, str) Name of product
    :param market (Optional, str) market name
    :param trade_id (Optional, str) trade id
    :param dt_from (Optional, str) datetime from which to process the data
    :param dt_to (Optional, str) datetime up to which to process the data
    :param freq (Optional, str) sampling interval
    :param n_largest (Optional, str) n largest/smallest value to be used in aggregate function
    :param n_smallest (Optional, str) Name of column (price or volume)
    
    Returns result of user query 
    Raises exception if bad parameters 
    """
    if n_smallest is not None and n_largest is not None:
        raise HTTPException(status_code=400, detail="Bad Parameter, n_largest and n_smallest can't be used at same time")
    
    if agg == "vwap" and col == "volume":
        raise HTTPException(status_code=400, detail="Bad Parameter, Col should be price to calculate Volume weighted Average Price")

    query = await get_query(
        col=col,
        agg=agg,
        groupby=groupby,
        product_duration=product_duration,
        product_type=product_type,
        product=product,
        market=market,
        trade_id=trade_id,
        dt_from=dt_from,
        dt_to=dt_to,
        freq=freq,
        n_largest=n_largest,
        n_smallest=n_smallest
    )
    db = SetupDB()
    df = db.recieve(query)
    return json.dumps(df.to_dict('records'))


async def get_query(
    col: str, 
    agg: str, 
    groupby: Optional[str] = None, 
    product_duration: Optional[str] = None,
    product_type: Optional[str] = None,
    product: Optional[str] = None,
    market: Optional[str] = None,
    trade_id: Optional[int] = None,
    dt_from: Optional[str] = None,
    dt_to: Optional[str] = None,
    freq: Optional[str] = None,
    n_largest: Optional[str] = None,
    n_smallest: Optional[str] = None,
) -> str:    
    """
    Builds the sql query from user query
    :param col (str) Name of column (price or volume)
    :param agg (str) Name of aggregate function (min, max, avg, vwap)
    :param groupby (Optional, str) Name of column to be used in group by
    :param product_duration (Optional, str) value of product duration 
    :param product_type (Optional, str) value of product type
    :param product (Optional, str) Name of product
    :param market (Optional, str) market name
    :param trade_id (Optional, str) trade id
    :param dt_from (Optional, str) datetime from which to process the data
    :param dt_to (Optional, str) datetime up to which to process the data
    :param freq (Optional, str) sampling interval
    :param n_largest (Optional, str) n largest/smallest value to be used in aggregate function
    :param n_smallest (Optional, str) Name of column (price or volume)
    """
    sub_query = ""
    if dt_from:
        dt_from = datetime.datetime.strptime(dt_from, '%Y%m%d%H%M').strftime('%Y-%m-%d %H:%M')
        sub_query += f"AND execution_time >= '{dt_from}' \n"
    
    if dt_to:
        dt_to = datetime.datetime.strptime(dt_to, '%Y%m%d%H%M').strftime('%Y-%m-%d %H:%M')
        sub_query += f"AND execution_time < '{dt_to}' \n"
    
    if market:
        sub_query += f"AND market = '{market}' \n"
    
    if trade_id:
        sub_query += f"AND trade_id = '{trade_id}' \n"
    
    if product:
        sub_query += f"AND product = '{product}' \n"
    
    if product_type:
        sub_query += f"AND product_type = '{product_type}' \n"
    
    if product_duration:
        sub_query += f"AND product_duration = '{product_duration}' \n"
    
    view = ""
    if n_largest or n_smallest:
        order = "" if n_smallest is not None else "desc"
        rank = n_smallest if n_smallest is not None else n_largest
        view = f"""
            with raw as (
                select 
                    *,
                    row_number() over(order by {col} {order}) as rank
                from trading_data.trading_data_aggregated_1min
                where 1=1
                {sub_query}
            )
        """
        sub_query = f"AND rank <= '{rank}' \n"


    table = "trading_data.trading_data_aggregated_1min" if n_largest is None and n_smallest is None else "raw"

    if agg == "vwap":
        column = "sum(price * volume)/sum(volume) as vwap"
    else:
        column = f"{agg}({col}) as {agg}_{col}"

    if freq and groupby:
        seconds = timeparse(freq)
        query = f"""
            {view}
            select 
                {groupby}, FLOOR(EXTRACT(EPOCH from execution_time)/{seconds}) as time, {column}
            from {table}
            where 1=1
            {sub_query}
            group by {groupby}, time
        """
    elif groupby:
        query = f"""
            {view}
            select 
                {groupby}, {column}
            from {table}
            where 1=1
            {sub_query}
            group by {groupby}
        """
    else:
        query = f"""
            {view}
            select 
                {column}
            from {table}
            where 1=1
            {sub_query}
        """
    return query
   
