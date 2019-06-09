import datetime
import pandas as pd
import pickle as pkl
import os
import unittest

from pandas_report_tracer.utils.columns_to_work_with import OneInputToFinalOptimization, analyze_one_input_to_result

HERE = os.path.dirname(os.path.abspath(__file__))


class TestOneInputToFinalOptimizationAMS(unittest.TestCase):
    # check tests Readme.md
    ams_input_fixture = pd.read_csv(
        '{}/fixtures/input/AMSBillofLandingHeaders-2018-sample.csv.gz'.format(HERE), compression='gzip')
    ams_result_fixture = pd.read_csv(
        '{}/fixtures/result/filtered_AMSBillofLandingHeaders-2018-sample.csv.gz'.format(HERE), compression='gzip')
    ams_based_obj = OneInputToFinalOptimization(ams_input_fixture, ams_result_fixture, merging_cols=['index'])
    ams_based_obj.find_matching_cols()
    ams_based_obj.merge_input_to_final()
    ams_based_obj.final_df_to_work_with()
    ams_based_obj.columns_usage_percentage()
    ams_based_obj.set_slicing_cols()
    for slicing_col, dtype in ams_based_obj.slicing_cols.items():
        if 'date' in dtype:
            ams_based_obj.determine_date_range_filters(slicing_col)
        else:
            ams_based_obj.determine_category_col_filters(slicing_col)
    ams_based_obj.determine_best_slicing_col_filter()

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

    def test_set_slicing_cols(self):
        assert self.ams_based_obj.slicing_cols == {
            'actual_arrival_date': 'date',
            'carrier_code': 'string',
            'conveyance_id': 'string',
            'conveyance_id_qualifier': 'string',
            'estimated_arrival_date': 'date',
            'foreign_port_of_destination': 'string',
            'foreign_port_of_destination_qualifier': 'string',
            'foreign_port_of_lading': 'string',
            'foreign_port_of_lading_qualifier': 'string',
            'in_bond_entry_type': 'string',
            'manifest_unit': 'string',
            'measurement': 'integer',
            'measurement_unit': 'string',
            'mode_of_transportation': 'string',
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
            'estimated_arrival_date', ('one_year_period', datetime.date(2018, 4, 1)), 'date', 41373, 2.4170352645445097
        )
