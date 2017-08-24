"""
    Helper functions
"""

import pandas as pd
import numpy as np
from epivizws import db, app

def execute_query(query, params):
    """
        Helper function to execute queries

        Args:
            query: query string to execute
            params: query params

        Result:
            data frame of query result
    """
    if params is None:
        df = pd.read_sql(query, con=db.get_engine(app))
    elif len(params) == 2:
        query_db = query % (params[0], params[1])
        df = pd.read_sql(query_db, con=db.get_engine(app))      
    else:
        query_db = query % (params[0], params[1], params[2], params[3], params[4])
        df = pd.read_sql(query_db, con=db.get_engine(app))

    return df

def bin_rows(input, max_rows=400):
    """
        Helper function to scale rows to resolution

        Args:
            input: dataframe to bin
            max_rows: resolution to scale rows

        Return:
            data frame with scaled rows
    """

    input = input.sort_values(["chr", "start"])
    input_length = len(input)

    if input_length < max_rows:
        return input

    step = max_rows
    col_names = input.columns.values.tolist()

    input["rowGroup"] = range(0, input_length)
    # input["rowGroup"] = input["rowGroup"].apply(lambda x: x / step)
    input["rowGroup"] = pd.cut(input["rowGroup"], bins=max_rows)
    input_groups = input.groupby("rowGroup")

    input_bin = []

    for name, group in input_groups:
        row = {}

        for col in col_names:
            if col in ["chr", "probe", "gene"]:
                row[col] = group[col].iloc[0]
            elif col in ["start", "id"]:
                row[col] = group[col].min()
            elif col == "end":
                row[col] = group[col].max()
            else:
                row[col] = group[col].mean()

        input_bin.append(row)

    bin_input = pd.DataFrame(input_bin)
    return bin_input

def format_result(input, params):
    globalStartIndex = None
    if len(input) > 0:
        globalStartIndex = input["id"].values.min()

    data = {
        "rows": {
            "globalStartIndex": globalStartIndex,
            "useOffset" : False,
            "values": {
                "id": None,
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
        col_names = input.columns.values.tolist()
        row_names = ["chr", "start", "end", "strand", "id"]

        for col in col_names:
            if params.get("measurement") is not None and col in params.get("measurement"):
                data["values"]["values"] = input[col].values.tolist()
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


    data["rows"]["values"]["id"] = None

    if params.get("datasource") != "genes":
        data["rows"]["values"]["strand"] = None

    return data
