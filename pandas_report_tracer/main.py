"""
clean your input DF so that it doesn't contain columns with different names but same information
"""
import glob

import pandas as pd

from utils.columns_to_work_with import OneInputToFinalOptimization, analyze_one_input_to_result
from utils.report_generation import generate_data_usage_plot, print_report

INPUT_PATH = "/tmp/input"
RESULT_PATH = "/tmp/result"
MERGING_DICT = {
    "/tmp/input/committed_revisions_all.csv": ["revisionId"]
}
RENAMING_COLS_DICT = {
    "/tmp/input/example.csv": {"revision_id": "revisionId"}
}


if __name__ == "__main__":
    result_file = glob.glob(RESULT_PATH + "/*.csv")
    resulting_df = pd.read_csv(result_file[0], escapechar='\\')
    filenames = glob.glob(INPUT_PATH + "/*.csv")
    for csv_file in filenames:
        input_df = pd.read_csv(csv_file, escapechar='\\')
        if csv_file in RENAMING_COLS_DICT:
            input_df = input_df.rename(columns=RENAMING_COLS_DICT[csv_file])
        if csv_file in MERGING_DICT:
            x = OneInputToFinalOptimization(input_df, resulting_df, MERGING_DICT[csv_file])
        else:
            x = OneInputToFinalOptimization(input_df, resulting_df)
        analyze_one_input_to_result(x)

        data_usage_plot = generate_data_usage_plot(x)
        print_report(x, filenames[0], result_file[0], data_usage_plot)
