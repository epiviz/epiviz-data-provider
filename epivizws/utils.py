"""
    Helper functions
"""

import pandas as pd
import numpy as np
from epivizws import db, app

def execute_query(query, params, type="data"):
    """
        Helper function to execute queries

        Args:
            query: query string to execute
            params: query params

        Result:
            data frame of query result
    """

    if type is "search":
        if params:
            df = pd.read_sql(query, con=db.get_engine(app), params=params)
        else:
            df = pd.read_sql(query, con=db.get_engine(app))
        return df
    # if params is None:
    #     df = pd.read_sql(query, con=db.get_engine(app))
    # else:
    #     df = pd.read_sql(query, con=db.get_engine(app), params=params)
    # return df

    if params is None:
        df = pd.read_sql(query, con=db.get_engine(app))
    elif len(params) == 2:
        query_db = query % (params[0], params[1])
        df = pd.read_sql(query_db, con=db.get_engine(app))    
    else:
        query_db = query % (params[0], params[1], params[2], params[3], params[4])
        df = pd.read_sql(query_db, con=db.get_engine(app))

    return df

def bin_rows(input, max_rows=2000):
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

def format_result(input, params, offset = True):
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
        globalStartIndex = input["id"].values.min()
        
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
