"""
clean your input DF so that it doesn't contain columns with different names but same information
"""
import datetime
import glob
import logging

import jinja2
import pandas as pd
from plotly.offline import plot
import plotly.graph_objs as go

from utils.columns_to_work_with import OneInputToFinalOptimization

INPUT_PATH = "/tmp/input"
RESULT_PATH = "/tmp/result"
MERGING_DICT = {
    "/tmp/input/committed_revisions_all.csv": ["revisionId"]
}
RENAMING_COLS_DICT = {
    "/tmp/input/example.csv": {"revision_id": "revisionId"}
}


def calculate_and_filter():
    import pudb; pudb.set_trace()
    result_file = glob.glob(RESULT_PATH + "/*.csv")
    resulting_df = pd.read_csv(result_file[0], escapechar='\\')
    filenames = glob.glob(INPUT_PATH + "/*.csv")
    input_df = pd.read_csv(filenames[0], escapechar='\\')
    if filenames[0] in RENAMING_COLS_DICT:
        input_df = input_df.rename(columns=RENAMING_COLS_DICT[filenames[0]])
    if filenames[0] in MERGING_DICT:
        x = OneInputToFinalOptimization(input_df, resulting_df, MERGING_DICT[filenames[0]])
    else:
        x = OneInputToFinalOptimization(input_df, resulting_df)
    # filter input DF with best filter found in analisis
    if x.best_filter[2] == 'date':
        nan_rows = input_df.loc[input_df[x.best_filter[0]].isnull()]
        input_df = input_df.loc[input_df[x.best_filter[0]] > x.best_filter[1]]
        input_df = pd.concat([input_df, nan_rows])
    else:
        input_df = input_df.loc[input_df[x.best_filter[0]] != x.best_filter[1]]
    input_df.to_csv(filenames[0], index=False, encoding='utf-8', escapechar='\\')
    

if __name__ == "__main__":
    for _ in range(2):
        calculate_and_filter()
