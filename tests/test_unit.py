
import pandas as pd
import unittest

from pandas_report_tracer.utils.columns_to_work_with import OneInputToFinalOptimization


class TestOneInputToFinalOptimization(unittest.TestCase):

    def test_find_matching_cols_no_ids(self):
        input_df = pd.DataFrame(columns=['column1', 'column2', 'column3'])
        resulting_df = pd.DataFrame(columns=['column1', 'column3', 'id1'])
        instance = OneInputToFinalOptimization(input_df, resulting_df)
        instance.find_matching_cols()
        assert instance.matching_cols == {'column1', 'column3'}
        assert instance.matching_id_cols == []

    def test_find_matching_cols_w_ids(self):
        input_df = pd.DataFrame(columns=['column1', 'column2', 'column3', 'id1'])
        resulting_df = pd.DataFrame(columns=['column1', 'column3', 'id1'])
        instance = OneInputToFinalOptimization(input_df, resulting_df)
        instance.find_matching_cols()
        assert list(instance.matching_cols) == ['column1', 'column2', 'column3', 'id1']
        assert list(instance.matching_id_cols) == ['id1']
