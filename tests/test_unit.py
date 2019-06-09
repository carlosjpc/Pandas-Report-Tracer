import numpy as np
import pandas as pd
import os
import unittest

from pandas.util.testing import assert_frame_equal

from pandas_report_tracer.utils.columns_to_work_with import OneInputToFinalOptimization

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
        instance.final_df_to_work_with()
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
        assert OneInputToFinalOptimization.isin_row(row_1, self.input_df)
        assert not OneInputToFinalOptimization.isin_row(row_2, self.input_df)


class TestOneInputToFinalOptimizationAMS(unittest.TestCase):
    # check tests Readme.md
    ams_input_fixture = pd.read_csv(
        '{}/fixtures/input/AMSBillofLandingHeaders-2018-sample.csv.gz'.format(HERE), compression='gzip')
    ams_result_fixture = pd.read_csv(
        '{}/fixtures/result/filtered_AMSBillofLandingHeaders-2018-sample.csv.gz'.format(HERE), compression='gzip')
    ams_based_obj = OneInputToFinalOptimization(ams_input_fixture, ams_result_fixture, merging_cols=['Index'])
    ams_based_obj.find_matching_cols()
    ams_based_obj.merge_input_to_final()
    ams_based_obj.final_df_to_work_with()

    def test_ams_find_matching_cols(self):
        assert set(self.ams_based_obj.matching_cols) == {
            'index', 'actual_arrival_date', 'port_of_unlading', 'vessel_name', 'weight', 'estimated_arrival_date',
            'foreign_port_of_lading_qualifier', 'manifest_unit', 'foreign_port_of_lading'
        }

    def test_ams_final_df_to_work_with(self):
        assert set(self.ams_based_obj.final_df.columns) == {
            'actual_arrival_date', 'estimated_arrival_date', 'foreign_port_of_lading',
            'foreign_port_of_lading_qualifier', 'index', 'manifest_unit', 'port_of_unlading', 'vessel_name', 'weight'
        }

    def test_ams_columns_usage_percentage(self):
        self.ams_based_obj.columns_usage_percentage()
        assert self.ams_based_obj.usage_percentage == {
            'actual_arrival_date': 0.13725490196078433,
            'estimated_arrival_date': 0,
            'foreign_port_of_lading': 0.4827586206896552,
            'foreign_port_of_lading_qualifier': 1.0,
            'index': 0.0012,
            'manifest_unit': 0.6419753086419753,
            'port_of_unlading': 0.2736842105263158,
            'vessel_name': 0.31327433628318585,
            'weight': 0.20916052183777653
        }
