import numpy as np
import pandas as pd
import unittest

from pandas.util.testing import assert_frame_equal

from pandas_report_tracer.utils.columns_to_work_with import OneInputToFinalOptimization


class TestOneInputToFinalOptimization(unittest.TestCase):

    def test_find_matching_cols_no_ids(self):
        input_df = pd.DataFrame(columns=['column1', 'column2', 'column3'])
        resulting_df = pd.DataFrame(columns=['column1', 'column3', 'id1'])
        instance = OneInputToFinalOptimization(input_df, resulting_df)
        instance.find_matching_cols()
        assert set(instance.matching_cols) == {'column1', 'column3'}
        assert instance.matching_id_cols == []

    def test_find_matching_cols_w_ids(self):
        input_df = pd.DataFrame(columns=['column1', 'column2', 'column3', 'id1'])
        resulting_df = pd.DataFrame(columns=['column1', 'column3', 'id1'])
        instance = OneInputToFinalOptimization(input_df, resulting_df)
        instance.find_matching_cols()
        assert set(instance.matching_cols) == {'id1', 'column3', 'column1'}
        assert instance.matching_id_cols == ['id1']

    def test_merge_input_to_final(self):
        input_df = pd.DataFrame({
            'column1': [1, 2],
            'column2': ['whateves', 'lolz'],
            'id1': ['key1.1', 'key1.3'],
            'id2': ['key2.1', 'key2.2']
        })
        resulting_df = pd.DataFrame({
            'column3': ['bla', 'bla'],
            'id1': ['key1.1', 'key1.2'],
            'id2': ['key2.1', 'key2.2']
        })
        instance = OneInputToFinalOptimization(input_df, resulting_df)
        instance.find_matching_cols()
        instance.merge_input_to_final()
        expected_df = pd.DataFrame({
            'column3': ['bla', 'bla'],
            'id1': ['key1.1', 'key1.2'],
            'id2': ['key2.1', 'key2.2'],
            'column1': [1, np.nan],
            'column2': ['whateves', np.nan],
        })
        assert_frame_equal(instance.extended_resulting_df, expected_df)
