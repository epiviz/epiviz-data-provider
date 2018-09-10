"""
    Helper functions
"""

import pandas as pd
import numpy as np
# from epivizws import loop
# import aiomysql
from epivizws.config import app_config
import pymysql
import ujson

async def execute_query(query, params, type="data"):
    """
        Helper function to execute queries

        Args:
            query: query string to execute
            params: query params

        Result:
            data frame of query result
    """

    connection = pymysql.connect(host='localhost',
                             user='root',
                             password='sarada',
                             db='epiviz',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

    if type is "search":
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            df = pd.DataFrame.from_records(result)
            return df

    if params is None:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    elif len(params) == 3:
        query_db = query % (params[0], params[1], params[2])
        with connection.cursor() as cursor:
            cursor.execute(query_db)
            result = cursor.fetchall()
    else:
        query_db = query % (params[0], params[1], params[2], params[3], params[4], params[5])
        with connection.cursor() as cursor:
            cursor.execute(query_db)
            result = cursor.fetchall()
        
    df = pd.DataFrame.from_records(result)

    return df

async def bin_rows(input, max_rows=2000):
    """
        Helper function to scale rows to resolution

        Args:
            input: dataframe to bin
            max_rows: resolution to scale rows

        Return:
            data frame with scaled rows
    """

    input_length = len(input)

    if input_length < max_rows:
        return input

    step = max_rows
    col_names = input.columns.values.tolist()

    input["rowGroup"] = range(0, input_length)
    input["rowGroup"] = pd.cut(input["rowGroup"], bins=max_rows)
    input_groups = input.groupby("rowGroup")

    agg_dict = {}

    for col in col_names:
        if col in ["chr", "probe", "gene", "region"]:
            agg_dict[col] = 'first'
        elif col in ["start", "id"]:
            agg_dict[col] = 'min'
        elif col == "end":
            agg_dict[col] = 'max'
        else:
            agg_dict[col] = 'mean'

    bin_input = input_groups.agg(agg_dict)

    return bin_input

async def format_result(input, params, offset = True):
    globalStartIndex = None
    data = {
        "rows": {
            "globalStartIndex": globalStartIndex,
            "useOffset" : offset,
            "values": {
                "id": None,
                "chr": [],
                "strand": [],
                "metadata": {}
            }
        },
        "values": {
            "globalStartIndex": globalStartIndex,
            "values": {}
        }
    }

    if len(input) > 0:
        globalStartIndex = int(input["id"].values.min())

        if offset:
            minStart = input["start"].iloc[0]
            minEnd = input["end"].iloc[0]
            input["start"] = input["start"].diff()
            input["end"] = input["end"].diff()
            input["start"].iloc[0] = minStart
            input["end"].iloc[0] = minEnd

        col_names = input.columns.values.tolist()
        row_names = ["chr", "start", "end", "strand", "id"]

        data = {
            "rows": {
                "globalStartIndex": globalStartIndex,
                "useOffset" : offset,
                "values": {
                    "id": None,
                    "chr": [],
                    "strand": [],
                    "metadata": {}
                }
            },
            "values": {
                "globalStartIndex": globalStartIndex,
                "values": {}
            }
        }

        for col in col_names:
            if params.get("measurement") is not None and col in params.get("measurement"):
                data["values"]["values"][col] = input[col].values.tolist()
                # data["values"]["values"][col] = data["values"]["values"][col].astype("float")
            elif col in row_names:
                data["rows"]["values"][col] = input[col].values.tolist()
                if col in ["id", "start", "end"]:
                    data["rows"]["values"][col] = input[col].values.tolist()
            else:
                data["rows"]["values"]["metadata"][col] = input[col].values.tolist()
    else:
        data["rows"]["values"]["start"] = []
        data["rows"]["values"]["end"] = []

        if params.get("metadata") is not None:
            for met in params.get("metadata"):
                data["rows"]["values"]["metadata"][met] = []
        # else:
        #     data["rows"]["values"]["metadata"] = None

    data["rows"]["values"]["id"] = None

    if params.get("datasource") != "genes":
        data["rows"]["values"]["strand"] = None

    return data
