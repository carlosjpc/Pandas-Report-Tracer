import numpy as np
import pandas as pd
import os
import unittest

from pandas.util.testing import assert_frame_equal

from pandas_report_tracer.utils.columns_to_work_with import OneInputToFinalOptimization, isin_row

HERE = os.path.dirname(os.path.abspath(__file__))


def df_equal_without_column_order(df1, df2):
    try:
        assert_frame_equal(df1.sort_index(axis=1), df2.sort_index(axis=1), check_names=True)
    except (AssertionError, ValueError, TypeError) as e:
        raise e
    else:
        return 1


class TestOneInputToFinalOptimization(unittest.TestCase):

    input_df = pd.DataFrame({
        'column1': [1, 2, 3, 4, 5],
        'column2': ['whateves', 'lolz', 'writing', 'tests', 'hard'],
        'id1': ['key1.1', 'key1.3', 'key1.3', 'key1.4',  np.nan],
        'id2': ['key2.1', 'key2.2', 'key2.3', 'key2.4', 'key2.5']
    })
    resulting_df = pd.DataFrame({
        'column3': ['uncle', 'bob', 'partitioning', 'tdd'],
        'id1': ['key1.1', 'key1.2', 'key1.3', 'key1.4'],
        'id2': ['key2.1', 'key2.2', 'key2.3', 'key2.5']
    })

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
        instance = OneInputToFinalOptimization(self.input_df, self.resulting_df)
        instance.find_matching_cols()
        instance.merge_input_to_final()
        expected_df = pd.DataFrame({
            'column3': ['uncle', 'bob', 'partitioning', 'tdd'],
            'id1': ['key1.1', 'key1.2', 'key1.3', 'key1.4'],
            'id2': ['key2.1', 'key2.2', 'key2.3', 'key2.5'],
            'column1': [1, np.nan, 3, np.nan],
            'column2': ['whateves', np.nan, 'writing', np.nan],
        })
        assert df_equal_without_column_order(instance.extended_resulting_df, expected_df)

    def test_merge_input_to_final_w_merging_cols(self):
        instance2 = OneInputToFinalOptimization(self.input_df, self.resulting_df, merging_cols=['id2'])
        instance2.find_matching_cols()
        instance2.merge_input_to_final()
        expected_df = pd.DataFrame({
            'column3': ['uncle', 'bob', 'partitioning', 'tdd'],
            'id1': ['key1.1', 'key1.2', 'key1.3', 'key1.4'],
            'id2': ['key2.1', 'key2.2', 'key2.3', 'key2.5'],
            'column1': [1, 2, 3, 5],
            'column2': ['whateves', 'lolz', 'writing', 'hard'],
        })
        assert df_equal_without_column_order(instance2.extended_resulting_df, expected_df)

    def test_columns_usage_percentage(self):
        instance = OneInputToFinalOptimization(self.input_df, self.resulting_df, merging_cols=['id2'])
        instance.find_matching_cols()
        instance.merge_input_to_final()
        instance.set_final_df_to_work_with()
        instance.columns_usage_percentage()
        assert {'column1': 0.80, 'column2': 0.80, 'id1': 0.75, 'id2': 0.80} == instance.usage_percentage
        assert 0.7875000000000001 == instance.overall_percentage

    def test_is_in_row(self):
        rows = pd.DataFrame({
            'column1': [2, np.nan],
            'column2': ['lolz', 'partitioning'],
            'id1': ['key1.3', 'key1.4'],
            'id2': ['key2.2', 'key2.3']
        })
        row_1 = rows.iloc[[0]]
        row_2 = rows.iloc[[1]]
        assert isin_row(row_1, self.input_df)
        assert not isin_row(row_2, self.input_df)
