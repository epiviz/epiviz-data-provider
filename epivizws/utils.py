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

    # conn = await aiomysql.connect(
    #             charset='utf8mb4',
    #             cursorclass=pymysql.cursors.DictCursor,
    #             host=app_config.get("host"), 
    #             # port=app_config.get("port"),
    #             user=app_config.get("user"), 
    #             password=app_config.get("password"), 
    #             # unix_socket=app_config.get("unix_socket"),
    #             db=app_config.get("db"), 
    #             loop=loop)

    connection = pymysql.connect(host='localhost',
                             user='root',
                             password='sarada',
                             db='epiviz',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

    # cur = await conn.cursor()
    print(type)
    print(query)
    print(params)

    if type is "search":
        # await cur.execute(query % (params))
        # result = await cur.fetchall()
        with connection.cursor() as cursor:
            # Read a single record
            sql = "show databases"
            cursor.execute(sql)
            result = cursor.fetchall()
            df = pd.DataFrame.from_records(result)
            return df

    if params is None:
        # await cur.execute(query)
        # result = await cur.fetchall()
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            print(result)
    elif len(params) == 3:
        query_db = query % (params[0], params[1], params[2])
        # await cur.execute(query_db)
        # result = await cur.fetchall()
        with connection.cursor() as cursor:
            cursor.execute(query_db)
            result = cursor.fetchall()
            print(result)
    else:
        query_db = query % (params[0], params[1], params[2], params[3], params[4], params[5])
        # await cur.execute(query_db)
        # result = await cur.fetchall()
        with connection.cursor() as cursor:
            cursor.execute(query_db)
            result = cursor.fetchall()
            print(result)
        
    df = pd.DataFrame.from_records(result)
    # await cur.close()
    # conn.close()
    print(df)
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

    # input = pd.read_json(ujson.dumps(input), orient="records")
    # print("input after json")
    # print(input)

    if len(input) > 0:
        globalStartIndex = input["id"].values.min()

        measurement = params.get("measurement")[0]

        col_names = input.columns.values.tolist()
        input_json = []
        for index, row in input.iterrows():
            rowObj = {}
            for col in col_names:
                rowObj[col] = row[col]
                if col == measurement:
                    rowObj[col] = round(row[col], 2)
                elif col in ["start", "end"]:
                    rowObj[col] = round(row[col], 0)
            
            input_json.append(rowObj)

        input = pd.read_json(ujson.dumps(input_json), orient="records")
        input = input.round({measurement: 2})
        input[measurement] = input[measurement].astype("int")

        if offset:
            minStart = input["start"].iloc[0]
            minEnd = input["end"].iloc[0]
            input["start"] = input["start"].diff()
            input["end"] = input["end"].diff()
            # input.start = input.start.astype("int")
            # input.end = input.end.astype("int")
            input["start"].iloc[0] = minStart
            input["end"].iloc[0] = minEnd

        input.start = input.start.astype("int")
        input.end = input.end.astype("int")
        input.chr = input.chr.astype("str")
        input.id = input.id.astype("int")

        col_names = input.columns.values.tolist()
        row_names = ["chr", "start", "end", "strand", "id"]

        print(input.dtypes)

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
            elif col in row_names:
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
