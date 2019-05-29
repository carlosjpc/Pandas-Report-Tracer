"""
clean your input DF so that it doesn't contain columns with different names but same information
"""
import datetime
import logging

import pandas as pd

from utils.columns_to_work_with import OneInputToFinalOptimization


def read_input(filename, input_directory='/tmp/input'):
    try:
        input_df = pd.read_csv('{}/{}.csv'.format(input_directory, filename),
                               escapechar='\\')
    except Exception:
        logging.warning('{} not found in {}'.format(filename, input_directory))
        return None
    return input_df


if __name__ == "__main__":
    # input_filenames = {'file_name': 'rename_columns_dictionary'}
    input_df = pd.read_csv('/tmp/input/committed_revisions_all.csv', escapechar='\\')
    resulting_df = pd.read_csv('/tmp/result/item_transactions.csv', escapechar='\\')
    # for input_filename, rename_dict in input_filenames.items():
    x = OneInputToFinalOptimization(input_df, resulting_df, ['revisionId'])
    