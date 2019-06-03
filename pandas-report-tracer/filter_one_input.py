"""
Small advise: clean your input DF so that it doesn't contain columns with different names but same information
"""
import glob
import logging

import pandas as pd

from utils.columns_to_work_with import OneInputToFinalOptimization, analyze_one_input_to_result, filter_and_save_inputfile
from utils.report_generation import print_report, generate_data_usage_plot

logger = logging.getLogger()
logger.setLevel(logging.INFO)

INPUT_PATH = "/tmp/input"
MAX_NUMBER_OF_RUNS = 3
MIN_BENEFIT_RATIO = 0.2
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
    input_df = pd.read_csv(filenames[0], escapechar='\\')
    if filenames[0] in RENAMING_COLS_DICT:
        input_df = input_df.rename(columns=RENAMING_COLS_DICT[filenames[0]])
    results = dict()
    for run_number in range(MAX_NUMBER_OF_RUNS):
        logging.info("-----------------"*5)
        logging.info("Starting run: {}".format(str(run_number + 1)))
        if run_number == 0:
            input_df_row_num = len(input_df)
        else:
            input_df = results[run_number - 1].input_df
        if filenames[0] in MERGING_DICT:
            results[run_number] = OneInputToFinalOptimization(input_df, resulting_df, MERGING_DICT[filenames[0]])
        else:
            results[run_number] = OneInputToFinalOptimization(input_df, resulting_df)
        analyze_one_input_to_result(results[run_number])
        data_usage_plot = generate_data_usage_plot(results[run_number])
        print_report(results[run_number], filenames[0], result_file, data_usage_plot)
        if (results[run_number].max_weighted_benefit / input_df_row_num) > MIN_BENEFIT_RATIO:
            filter_and_save_inputfile(results[run_number], filenames[0])
            logging.info("Applied weighted benefit: {}".format(str(results[run_number].max_weighted_benefit)))
        else:
            break
