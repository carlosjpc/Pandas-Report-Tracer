import datetime
import functools
import itertools
import logging
import operator
import random

from functools import reduce

import pandas as pd
from pandas.api.types import is_string_dtype, is_integer_dtype, is_bool_dtype

NATURAL_DIVIDER_THRESOLD = 30
MULTIPLE_COMBINATION_FILTERS = 5000
MULTI_COL_FILTER_RATIO = 0.05
TODAY = datetime.date(2019,4,1)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class OneInputToFinalOptimization:
    combos_to_check_in_final = list()
    extended_resulting_df = pd.DataFrame()
    input_df = pd.DataFrame()
    input_df_cols = list()
    filtering_quick_gains = dict()
    matching_cols = set()
    matching_id_cols = list()
    merging_cols = list()
    resulting_df = pd.DataFrame()
    slicing_cols = dict()
    usage_percentage = dict()

    def __init__(self, input_df, resulting_df, merging_cols=None):
        logging.info("Starting Analisis")
        self.input_df = input_df
        self.resulting_df = resulting_df
        self.input_df_cols = input_df.columns
        self.merging_cols = merging_cols
        self.find_matching_cols()
        if self.matching_cols.empty:
            logging.warning('Without shared columns this tool is worthless, consider renaming columns')
            return
        self.final_df_to_work_with()
        self.columns_usage_percentage()
        logging.info("Column usage percentage:")
        logging.info(self.usage_percentage)
        self.set_slicing_cols()
        logging.info("Slicing cols:")
        logging.info(self.slicing_cols)
        for slicing_col, dtype in self.slicing_cols.items():
            if 'date' in dtype:
                self.determine_date_range_filters(slicing_col)
            else:
                self.determine_category_col_filters(slicing_col)
        self.determine_possible_multi_column_filters()
        self.determine_multi_column_filters()
        self.determine_best_slicing_col_filter()

    def find_matching_cols(self):
        # to identify a column for merge it must contain 'id' or 'Id'
        self.matching_cols = set(self.input_df.columns).intersection(self.resulting_df.columns)
        self.matching_id_cols = [col_name for col_name in self.matching_cols if 'id' in col_name or 'Id' in col_name]
        if self.matching_id_cols or self.merging_cols:
            try:
                self.merge_input_to_final()
            except Exception as e:
                logging.warning("The DFs have some 'id' columns to merge them but an exception araised when trying merge")
                logging.warning(e)
                pass
            else:
                self.matching_cols = self.input_df.columns
        else:
            logging.warning('The input df could not be merged into final, that decreases the chances of success')

    def merge_input_to_final(self):
        if self.merging_cols:
            input_df_cols = list((set(self.input_df.columns) - self.matching_cols))
            self.extended_resulting_df = self.resulting_df.merge(
                    self.input_df[input_df_cols + self.merging_cols],
                    how='left', on=self.merging_cols
                )
        else:
            input_df_cols = list(set(self.input_df.columns) - (self.matching_cols - set(self.matching_id_cols)))
            self.extended_resulting_df = self.resulting_df.merge(
                    self.input_df[input_df_cols],
                    how='left', on=self.matching_id_cols
                )

    def final_df_to_work_with(self):
        if not self.extended_resulting_df.empty:
            self.final_df = self.extended_resulting_df
        else:
            self.final_df = self.resulting_df

    def columns_usage_percentage(self):
        # the lower the percentage the more file you are reading in vain
        # a MySQL query adjustment or a reading the table using Athena would increase speed
        for col in self.matching_cols:
            if 'date' in col or is_bool_dtype(self.input_df[col]):
                pass
            matching_rows = set(self.input_df[col].unique()).intersection(self.final_df[col].unique())
            if not matching_rows:
                logging.info('{} is in both DFs, but no matching data was found'.format(col))
                percentage = 0
            else:
                percentage = len(matching_rows) / len(self.input_df[col].unique())
            self.usage_percentage.update({col: percentage})

    def set_slicing_cols(self):
        for col in self.input_df.columns:
            if 'date' in col or 'Date' in col:
                if self.convert_input_col_to_date(col):
                    self.slicing_cols.update({col: 'date'})
            elif is_string_dtype(self.input_df[col]):
                if self.is_natural_divider(self.input_df[col]):
                    self.slicing_cols.update({col: 'string'})
            elif is_integer_dtype(self.input_df[col]):
                if self.is_natural_divider(self.input_df[col]):
                    self.slicing_cols.update({col: 'integer'})
            elif is_bool_dtype(self.input_df[col]):
                self.slicing_cols.update({col: 'boolean'})

    @staticmethod
    def is_natural_divider(df_series):
        # if a string or int column can be used as divider
        unique_rows = len(df_series.unique())
        total_rows = len(df_series)
        ratio = total_rows / unique_rows
        logging.info('Column: {} has {} unique rows in {} rows, a {} to 1 relationship'.format(
            df_series._name, str(unique_rows), str(total_rows), str(ratio)))
        if ratio > NATURAL_DIVIDER_THRESOLD:
            return 1

    def determine_date_range_filters(self, col):
        date_slices = {
            'one_month_period': TODAY - datetime.timedelta(days=183),
            'one_quarter_period': TODAY - datetime.timedelta(days=183),
            'six_months_period': TODAY - datetime.timedelta(days=183),
            'one_year_period': TODAY - datetime.timedelta(days=365),
            'mtd': datetime.date(TODAY.year, TODAY.month, 1),
            'ytd': datetime.date(TODAY.year, 1, 1)
        }
        self.final_df[col] = pd.to_datetime(self.final_df[col])
        for period, date_ in date_slices.items():
            lesser_date_input = self.input_df.loc[self.input_df[col] < date_]
            if len(lesser_date_input) and col in self.final_df.columns:
                lesser_date_final = self.final_df.loc[self.final_df[col] < date_]
                if len(lesser_date_final) == 0:
                    logging.info('Found query optimizing chance in col: {}, filter: {}'.format(col, period))
                    self.filtering_quick_gains.update(
                        {col: {
                            'column': col,
                            'dtype': 'date',
                            'filter_out': period,
                            'useless_rows': len(lesser_date_input),
                            'weighted_benefit': len(lesser_date_input) - NATURAL_DIVIDER_THRESOLD*1
                        }}
                    )

    def determine_category_col_filters(self, col):
        if self.usage_percentage[col] == 1:
            return
        unused_categos = set(self.input_df[col].unique()) - set(self.final_df[col].unique())
        unused_inputdf_rows = len(self.input_df.loc[self.input_df[col].isin(unused_categos)])
        self.filtering_quick_gains.update(
                {col: {
                    'column': col,
                    'dtype': self.slicing_cols[col],
                    'filter_out': unused_categos,
                    'useless_rows': unused_inputdf_rows,
                    'weighted_benefit': unused_inputdf_rows - NATURAL_DIVIDER_THRESOLD*len(unused_categos)
                }}
        )
        if len(unused_categos) > 100:
            logging.info(
                "Found query optimizing chance in col: {}, reading ({}) unused rows, consider filtering out: {}".format(
                    col, unused_inputdf_rows, random.sample(unused_categos, 100)))
        else:
            logging.info(
                "Found query optimizing chance in col: {}, reading ({}) unused rows, consider filtering out: {}".format(
                    col, unused_inputdf_rows, unused_categos))

    @staticmethod
    def isin_row(row, df):
        cols = df.columns
        bool_series = functools.reduce(lambda x, y: x & y, [df[col].isin(row[col]) for col in cols])
        return bool_series.any()

    def combo_appears_often_in_input(self, row, input_catego_df):
        df = input_catego_df.get_group(row)
        multi_col_filter_ratio = len(df) / len(self.input_df)
        if multi_col_filter_ratio > MULTI_COL_FILTER_RATIO:
            return 1
        else:
            return 0

    def determine_possible_multi_column_filters(self):
        combinations_generator = self.generate_possible_combinations()
        input_catego_df = self.input_df[self.catego_cols].drop_duplicates()
        for combo in combinations_generator:
            combo_row_df = pd.DataFrame([combo], columns=input_catego_df.columns)
            if self.isin_row(combo_row_df, input_catego_df):
                self.combos_to_check_in_final.append((combo_row_df, combo))

    def generate_possible_combinations(self):
        lists_for_prod = list()
        if self.use_all_slicing_cols_as_catego_cols():
            self.catego_cols = self.slicing_cols
        else:
            self.set_catego_columns()
        logging.info("Category Columns:")
        logging.info(self.catego_cols)
        for col in self.catego_cols:
            unique_rows = list(self.input_df[col].unique())
            lists_for_prod.append(unique_rows)
        return itertools.product(*lists_for_prod)

    def determine_multi_column_filters(self):
        final_catego_df = self.final_df[self.catego_cols].drop_duplicates()
        input_catego_df = self.input_df.groupby(self.catego_cols)
        combos_to_exclude = list()
        for combo_row in self.combos_to_check_in_final:
            if (self.isin_row(combo_row[0], final_catego_df) and
                    self.combo_appears_often_in_input(combo_row[1], input_catego_df)):
                combos_to_exclude.append(combo_row[0])
        self.combos_to_exclude = pd.concat(combos_to_exclude)

        logging.info("""
        Of the {} value combinations across columns: {}; {} don't show in the final DF, consider filtering those out.
        """.format(
            len(self.combos_to_check_in_final),
            self.combos_to_exclude.columns,
            len(self.combos_to_exclude)
        ))
        logging.info(self.combos_to_exclude)

    def use_all_slicing_cols_as_catego_cols(self):
        # because this will be used to generate combinations (which grow quiet fast) we need to be more careful
        number_of_combinations = 1
        for col in self.slicing_cols:
            number_of_combinations *= len(self.input_df[col].unique())
            if number_of_combinations > MULTIPLE_COMBINATION_FILTERS:
                return 0
        return 1

    def set_catego_columns(self):
        slicing_cols_unique_length = dict()
        for col in self.slicing_cols:
            slicing_cols_unique_length.update({col: len(self.input_df[col].unique())})
        number_of_combinations = reduce(lambda x, y: x*y, slicing_cols_unique_length.values())
        while number_of_combinations > MULTIPLE_COMBINATION_FILTERS:
            max_key = max(slicing_cols_unique_length.items(), key=operator.itemgetter(1))[0]
            del slicing_cols_unique_length[max_key]
            number_of_combinations = reduce(lambda x, y: x*y, slicing_cols_unique_length.values())
        self.catego_cols = list(slicing_cols_unique_length.keys())

    def convert_input_col_to_date(self, col_name):
        try:
            self.input_df[col_name] = pd.to_datetime(self.input_df[col_name])
        except Exception:
            return False
        return True

    def find_largest_unused_catego_in_column(self, col):
        unused_categos = set(self.input_df[col].unique()) - set(self.final_df[col].unique())
        category_potential = dict()
        for category in unused_categos:
            unused_rows_for_category = len(self.input_df.loc[self.input_df[col] == category])
            category_potential.update({category, unused_rows_for_category})
        return max(category_potential.iteritems(), key=operator.itemgetter(1))[0]

    def determine_best_slicing_col_filter(self):
        # two deciding factors: total number of rows that can be filtered (the larger the better),
        # the number of elements to filter out (the larger the worse)
        efficiency_indicator_list = list()
        for k, d_info in self.filtering_quick_gains.items():
            efficiency_indicator_list.append((d_info['column'], d_info['weighted_benefit']))
        max_weighted_benefit = max([x[1] for x in efficiency_indicator_list])
        max_eficiency_potential_col = [item for item in efficiency_indicator_list if item[1] == max_weighted_benefit][0][0]
        if self.filtering_quick_gains[max_eficiency_potential_col]['dtype'] != 'date':
            category_to_filter = self.find_largest_unused_catego_in_column(max_eficiency_potential_col)
            logging.info("The suggested column to filter is {}, recommended value to filter: {}".format(
                max_eficiency_potential_col, category_to_filter
            ))
        else:
            logging.info("The suggested column to filter is {}, recommended period to filter: {}".format(
                max_eficiency_potential_col, self.filtering_quick_gains[max_eficiency_potential_col]['filter_out']
            ))
        logging.info("Full efficiency analisis: {}".format(efficiency_indicator_list))
