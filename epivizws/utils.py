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
    else:
        # df = pd.read_sql(query, params=params, con=db.get_engine(app))
        # print params[0], params[1], params[2], params[3], params[4]
        query_db = query % (params[0], params[1], "'" + params[2] + "'", params[3], params[4])
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
    input_length = len(input)

    if input_length < max_rows:
        return input

    step = max_rows
    col_names = input.columns.values.tolist()

    input["rowGroup"] = range(0, input_length)
    input["rowGroup"] = input["rowGroup"].apply(lambda x: x / step)
    input_groups = input.groupby("rowGroup")

    input_bin = []

    print col_names

    for name, group in input_groups:
        print name
        print group
        row = {}

        for col in col_names:
            print col
            if col == "chr":
                row[col] = group[col].iloc[0]
            elif col == "start":
                row[col] = group[col].min()
            elif col == "end":
                row[col] = group[col].max()
            else:
                row[col] = group[col].mean()

        input_bin.append(row)

    bin_input = pd.DataFrame(input_bin)
    return bin_input
