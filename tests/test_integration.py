import datetime
import codecs
import pandas as pd
import pickle as pkl
import os
import unittest

from pandas_report_tracer.utils.columns_to_work_with import SingleColumnFilters, MultiColumnFilters

from pandas_report_tracer.utils.report_generation import generate_data_usage_plot

from tests.test_unit import df_equal_without_column_order

HERE = os.path.dirname(os.path.abspath(__file__))


class TestSingleColumnFiltersAMS(unittest.TestCase):
    # check tests Readme.md
    ams_input_fixture = pd.read_csv(
        '{}/fixtures/input/AMSBillofLandingHeaders-2018-sample.csv.gz'.format(HERE), compression='gzip')
    ams_result_fixture = pd.read_csv(
        '{}/fixtures/result/filtered_AMSBillofLandingHeaders-2018-sample.csv.gz'.format(HERE), compression='gzip')
    ams_based_obj = SingleColumnFilters(ams_input_fixture, ams_result_fixture, merging_cols=['index'])
    ams_based_obj.input_file_name = '/tmp/filtered_input.csv'
    ams_based_obj.make_analysis(apply_filter=True)

    def test_ams_find_matching_cols(self):
        assert set(self.ams_based_obj.matching_cols) == {
            'actual_arrival_date', 'carrier_code', 'conveyance_id', 'conveyance_id_qualifier', 'estimated_arrival_date',
            'foreign_port_of_destination', 'foreign_port_of_destination_qualifier', 'foreign_port_of_lading',
            'foreign_port_of_lading_qualifier', 'identifier', 'in_bond_entry_type', 'index', 'manifest_quantity',
            'manifest_unit', 'measurement', 'measurement_unit', 'mode_of_transportation', 'place_of_receipt',
            'port_of_destination', 'port_of_unlading', 'record_status_indicator', 'secondary_notify_party_1',
            'secondary_notify_party_10', 'secondary_notify_party_2', 'secondary_notify_party_3',
            'secondary_notify_party_4', 'secondary_notify_party_5', 'secondary_notify_party_6',
            'secondary_notify_party_7', 'secondary_notify_party_8', 'secondary_notify_party_9', 'vessel_country_code',
            'vessel_name', 'weight', 'weight_unit'
        }

    def test_ams_find_matching_id_cols(self):
        assert set(self.ams_based_obj.matching_id_cols) == set()

    def test_ams_columns_usage_percentage(self):
        assert self.ams_based_obj.usage_percentage == {
            'actual_arrival_date': 0.13725490196078433,
            'carrier_code': 0.041429731925264016,
            'conveyance_id': 0.03242742433600988,
            'conveyance_id_qualifier': 1.0,
            'estimated_arrival_date': 0,
            'foreign_port_of_destination': 0.01,
            'foreign_port_of_destination_qualifier': 0.5,
            'foreign_port_of_lading': 0.4827586206896552,
            'foreign_port_of_lading_qualifier': 1.0,
            'identifier': 0.0030211480362537764,
            'in_bond_entry_type': 0.5,
            'index': 0.0012,
            'manifest_quantity': 0.014442013129102845,
            'manifest_unit': 0.6419753086419753,
            'measurement': 0.02443857331571995,
            'measurement_unit': 0.4444444444444444,
            'mode_of_transportation': 1.0,
            'place_of_receipt': 0.010784823284823285,
            'port_of_destination': 0.06201550387596899,
            'port_of_unlading': 0.2736842105263158,
            'record_status_indicator': 0.6666666666666666,
            'secondary_notify_party_1': 0.04748982360922659,
            'secondary_notify_party_10': 0,
            'secondary_notify_party_2': 0.0546218487394958,
            'secondary_notify_party_3': 0.02857142857142857,
            'secondary_notify_party_4': 0.03333333333333333,
            'secondary_notify_party_5': 0.047619047619047616,
            'secondary_notify_party_6': 0.0625,
            'secondary_notify_party_7': 0.07692307692307693,
            'secondary_notify_party_8': 0.14285714285714285,
            'secondary_notify_party_9': 0,
            'vessel_country_code': 0.1743119266055046,
            'vessel_name': 0.31327433628318585,
            'weight': 0.20916052183777653,
            'weight_unit': 0.3333333333333333
        }

    def test_overall_percentage(self):
        assert self.ams_based_obj.overall_percentage == 0.23915826258701534

    def test_get_dtypes_for_natural_divider_cols(self):
        assert self.ams_based_obj.natural_dividers_dtypes == {
            'actual_arrival_date': 'date',
            'carrier_code': 'string',
            'conveyance_id': 'string',
            'estimated_arrival_date': 'date',
            'foreign_port_of_destination': 'string',
            'foreign_port_of_destination_qualifier': 'string',
            'foreign_port_of_lading': 'string',
            'in_bond_entry_type': 'string',
            'manifest_unit': 'string',
            'measurement': 'integer',
            'measurement_unit': 'string',
            'port_of_destination': 'string',
            'port_of_unlading': 'string',
            'record_status_indicator': 'string',
            'secondary_notify_party_1': 'string',
            'secondary_notify_party_2': 'string',
            'secondary_notify_party_3': 'string',
            'secondary_notify_party_4': 'string',
            'secondary_notify_party_5': 'string',
            'secondary_notify_party_6': 'string',
            'secondary_notify_party_7': 'string',
            'secondary_notify_party_8': 'string',
            'vessel_country_code': 'string',
            'vessel_name': 'string',
            'weight_unit': 'string'
        }

    def test_filtering_quick_gains(self):
        expected_result = pkl.load(open("{}/fixtures/test_results/expected_result.p".format(HERE), "rb"))
        assert expected_result == self.ams_based_obj.filtering_quick_gains

    def test_determine_best_slicing_col_filter(self):
        assert self.ams_based_obj.best_filter == (
            'estimated_arrival_date', ('one_year_period', datetime.date(2018, 4, 1)), 'date', 41373, 0.41373
        )

    def test_find_largest_unused_catego_in_column(self):
        x = self.ams_based_obj.find_largest_unused_catego_in_column('foreign_port_of_destination_qualifier')
        assert x == 'Schedule K Foreign Port'

    def test_filter_and_save_inputfile(self):
        filtered_input = pd.read_csv('/tmp/filtered_input.csv', encoding='utf-8', escapechar='\\')
        expected_filtered_input = pd.read_csv("{}/fixtures/test_results/expected_filtered_input.csv".format(HERE))
        assert df_equal_without_column_order(filtered_input, expected_filtered_input)


class TestMultiColumnFiltersAMS(unittest.TestCase):
    # check tests Readme.md
    ams_input_fixture = pd.read_csv(
        '{}/fixtures/input/AMSBillofLandingHeaders-2018-sample.csv.gz'.format(HERE), compression='gzip')
    ams_result_fixture = pd.read_csv(
        '{}/fixtures/result/filtered_AMSBillofLandingHeaders-2018-sample.csv.gz'.format(HERE), compression='gzip')
    ams_based_multi_col_obj = MultiColumnFilters(ams_input_fixture, ams_result_fixture, merging_cols=['index'])
    ams_based_multi_col_obj.get_multi_column_filters()

    def test_set_combo_columns(self):
        assert self.ams_based_multi_col_obj.combo_cols == [
            'weight_unit', 'record_status_indicator', 'foreign_port_of_destination_qualifier', 'in_bond_entry_type',
            'secondary_notify_party_8'
        ]
