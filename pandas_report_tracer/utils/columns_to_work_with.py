import datetime
import functools
import itertools
import logging
import operator
import random
import statistics

from functools import reduce

import pandas as pd
from pandas.api.types import is_string_dtype, is_integer_dtype, is_bool_dtype

NATURAL_DIVIDER_THRESOLD = 30
MULTIPLE_COMBINATION_FILTERS = 5000
MULTI_COL_FILTER_RATIO = 0.05
TODAY = datetime.date(2019, 4, 1)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def filter_and_save_inputfile(result_ob, input_file_name):
    """Applies the best filter, result of running 'analyze_one_input_to_result' to the input_df and saves it,
    replacing the original file.

    Parameters:
    result_ob (object): an object of the class OneInputToFinalOptimization
    input_file_name (string): the path and name of the input csv
    Returns:
    None
    """
    # filter input DF with best filter found in analisis
    if result_ob.best_filter[2] == 'date':
        nan_rows = result_ob.input_df.loc[result_ob.input_df[result_ob.best_filter[0]].isnull()]
        result_ob.input_df = result_ob.input_df.loc[
            result_ob.input_df[result_ob.best_filter[0]] > result_ob.best_filter[1][1]
        ]
        result_ob.input_df = pd.concat([result_ob.input_df, nan_rows])
    else:
        result_ob.input_df = result_ob.input_df.loc[
            result_ob.input_df[result_ob.best_filter[0]] != result_ob.best_filter[1]
        ]
    result_ob.input_df.to_csv(input_file_name, index=False, encoding='utf-8', escapechar='\\')


def analyze_one_input_to_result(one_input_to_final):
    """Runs functions of the class OneInputToFinalOptimization on an object of the class to get the best single
    column filter while also generating the data for the 'print_report' and 'generate_data_usage_plot'.

    Parameters:
    one_input_to_final (object): an object of the class OneInputToFinalOptimization
    Returns:
    None
    """
    logging.info("Starting Analisis")
    one_input_to_final.find_matching_cols()
    if one_input_to_final.matching_cols.empty:
        logging.warning('Without shared columns this tool is worthless, consider renaming columns')
        return
    one_input_to_final.merge_input_to_final()
    one_input_to_final.final_df_to_work_with()
    one_input_to_final.columns_usage_percentage()
    logging.info("Column usage percentage:")
    logging.info(one_input_to_final.usage_percentage)
    one_input_to_final.set_slicing_cols()
    logging.info("Slicing cols:")
    logging.info(one_input_to_final.slicing_cols)
    for slicing_col, dtype in one_input_to_final.slicing_cols.items():
        if 'date' in dtype:
            one_input_to_final.determine_date_range_filters(slicing_col)
        else:
            one_input_to_final.determine_category_col_filters(slicing_col)
    one_input_to_final.determine_possible_multi_column_filters()
    one_input_to_final.determine_multi_column_filters()
    one_input_to_final.determine_best_slicing_col_filter()


class OneInputToFinalOptimization:
    date_slices = {
        'one_month_period': TODAY - datetime.timedelta(days=31),
        'one_quarter_period': TODAY - datetime.timedelta(days=92),
        'six_months_period': TODAY - datetime.timedelta(days=183),
        'one_year_period': TODAY - datetime.timedelta(days=365),
        'mtd': datetime.date(TODAY.year, TODAY.month, 1),
        'ytd': datetime.date(TODAY.year, 1, 1)
    }

    def __init__(self, input_df, resulting_df, merging_cols=None):
        self.catego_cols = list()
        self.combos_to_check_in_final = list()
        self.combos_to_exclude = pd.DataFrame()
        self.extended_resulting_df = pd.DataFrame()
        self.filtering_quick_gains = list()
        self.final_df = pd.DataFrame()
        self.input_df = input_df
        self.input_df_cols = input_df.columns
        self.matching_cols = list()
        self.matching_id_cols = list()
        self.merging_cols = merging_cols
        self.resulting_df = resulting_df
        self.slicing_cols = dict()
        self.usage_percentage = dict()

    def find_matching_cols(self):
        """Compares the columns in 'input_df' and 'resulting_df' to find columns present in both DF, a second list is
        calculated for columns also having id or Id in their name, to be used for merging the DFs if no 'merging_cols'
        list is provided.

        """
        self.matching_cols = list(set(self.input_df.columns).intersection(self.resulting_df.columns))
        self.matching_id_cols = [col_name for col_name in self.matching_cols if 'id' in col_name or 'Id' in col_name]

    def merge_input_to_final(self):
        """Merges the 'input_df' to 'final_df', if a list of columns was passed to the instance the merge is executed
        on those columns, otherwise on all 'matching_id_cols'

        """
        input_df_cols = list(set(self.input_df.columns) - set(self.matching_cols))
        try:
            if self.merging_cols:
                self._merge_input_to_final_on_merging_cols(input_df_cols)
            else:
                self._merge_input_to_final_on_matching_id_cols(input_df_cols)
        except Exception as e:
            logging.warning("""The DFs have some 'id' columns to merge them but an exception
                                   araises when trying merge""")
            logging.warning(e)
            pass
        else:
            self.matching_cols = self.input_df.columns

    def _merge_input_to_final_on_merging_cols(self, input_df_cols):
        input_df_cols.extend(self.merging_cols)
        self.extended_resulting_df = self.resulting_df.merge(
            self.input_df[input_df_cols],
            how='left', on=self.merging_cols
        )

    def _merge_input_to_final_on_matching_id_cols(self, input_df_cols):
        input_df_cols.extend(self.matching_id_cols)
        self.extended_resulting_df = self.resulting_df.merge(
            self.input_df[input_df_cols],
            how='left', on=self.matching_id_cols
        )

    def final_df_to_work_with(self):
        """Checks if the 'input_df' was merged to 'final_df', to work with that merged DF, which increases the scope
        of the analysis because all columns are considered, otherwise only the 'matching cols'.

        """
        if not self.extended_resulting_df.empty:
            self.final_df = self.extended_resulting_df
        else:
            self.final_df = self.resulting_df
            logging.warning('The input df could not be merged into final, that decreases the chances of success')

    def columns_usage_percentage(self):
        """Preliminary analysis that measure the ratio of unique values in 'input_df' that make it to the 'final_df',
        the lower the percentage the more file you are reading in vain, in that case a MySQL query adjustment or
        reading the table using Athena to filter a bucket would increase speed.

        """
        for col in self.matching_cols:
            if 'date' in col or is_bool_dtype(self.input_df[col]):
                pass
            matching_rows = set(self.input_df[col].unique()).intersection(self.final_df[col].unique())
            if not matching_rows:
                logging.info('{} is in both DFs, but no matching data was found'.format(col))
                percentage = 0
                if col in self.merging_cols:
                    raise ValueError('No keys to perform merge')
            else:
                percentage = len(matching_rows) / len(self.input_df[col].unique())
            self.usage_percentage.update({col: percentage})
        self.overall_percentage = statistics.mean(self.usage_percentage.values())

    def set_slicing_cols(self):
        """For each column in 'input_df' determine its dtype (many columns have dtype object which is not useful
        for this analysis). And add to a dictionary if 'is_natural_divider'

        """
        for col in self.input_df.columns:
            if 'date' in col or 'Date' in col:
                if self.convert_str_col_to_date(col):
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
        """Determines if a column of the 'input_df' would serve as a good filter, if the ratio of unique values to
        the number of rows is high the column is consider a 'natural divider'

        :param df_series: (pandas series) series to be checked to see if it is a 'natural divider'
        :return: Boolean
        """
        # if a string or int column can be used as divider
        unique_rows = len(df_series.unique())
        total_rows = len(df_series)
        ratio = total_rows / unique_rows
        logging.info('Column: {} has {} unique rows in {} rows, a {} to 1 relationship'.format(
            df_series._name, str(unique_rows), str(total_rows), str(ratio)))
        if ratio > NATURAL_DIVIDER_THRESOLD:
            return 1

    def handle_na_in_date_cols(self, col):
        """This function checks for nans values in the 'input_df' DF for a given column, if there are such values
        in the 'input_df' but not in the 'final_df' it has found a valuable filter. It adds that finding to the
        'filtering_quick_gains' list.

        :param col: (str) the name of the column in the 'input_df'
        """
        number_of_nans = self.input_df[col].isna().sum()
        if number_of_nans and self.final_df[col].isna().sum() == 0:
            logging.info('Found query optimizing chance in col: {}, filter: nan'.format(col))
            self.filtering_quick_gains.append({
                'column': col,
                'dtype': 'date',
                'filter_out': 'nan',
                'useless_rows': number_of_nans,
                'weighted_benefit': number_of_nans
            })

    def determine_date_range_filters(self, col):
        """For each of the time ranges determined in the 'date_slices' class dictionary this functions checks if
        there is data in the 'final_df' if not it continues with the next date range, otherwise it checks if there is
        data for that same date range in the 'input_df', if so, there is un needed rows in the 'input_df' that should
        be filtered. That information is stored in function variables to determine the best date range filter that can
        be applied to that column, that information is stored in a dictionary and appended to 'filtering_quick_gains'
        an instance list.

        :param col: (str) the name of the column in the 'input_df'
        """
        self.handle_na_in_date_cols(col)
        non_na_inputdf = self.input_df.dropna(subset=[col])
        self.final_df[col] = pd.to_datetime(self.final_df[col])
        non_na_finaldf = self.final_df.dropna(subset=[col])
        weighted_benefit = 0
        for period, date_ in self.date_slices.items():
            lesser_date_final = non_na_finaldf.loc[non_na_finaldf[col] < date_]
            if not len(lesser_date_final):
                lesser_date_input = non_na_inputdf.loc[non_na_inputdf[col] < date_]
                if len(lesser_date_input) > weighted_benefit:
                    logging.info('Found query optimizing chance in col: {}, filter: {}'.format(col, period))
                    weighted_benefit = len(lesser_date_input)
                    the_column = col
                    filter_out = (period, date_)
                    useless_rows = len(lesser_date_input)
        if weighted_benefit > 0:
            self.filtering_quick_gains.append({
                'column': the_column,
                'dtype': 'date',
                'filter_out': filter_out,
                'useless_rows': useless_rows,
                'weighted_benefit': weighted_benefit
            })

    def determine_category_col_filters(self, col):
        """If a columns 'col' unique values are not fully present in the 'final_df' this function determines the
        unique values in the 'input_df' for the column 'col' not present in the 'resulting_df' / 'final_df' and
        adds that information to a instance dictionary for further analysis

        :param col: (str) the name of the column in the 'input_df'
        """
        if self.usage_percentage[col] == 1:
            return
        unused_categos = set(self.input_df[col].unique()) - set(self.final_df[col].unique())
        unused_inputdf_rows = len(self.input_df.loc[self.input_df[col].isin(unused_categos)])
        expected_unused_rows_per_catego = unused_inputdf_rows / len(unused_categos)
        self.filtering_quick_gains.append({
            'column': col,
            'dtype': self.slicing_cols[col],
            'filter_out': unused_categos,
            'useless_rows': unused_inputdf_rows,
            'weighted_benefit': expected_unused_rows_per_catego
        })
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
        """Given a 'row' (specific combination of values for the DF columns) this function searches the 'df'
        for that same combination of values, return True if the 'df' has that exact combination, False if otherwise.

        :param row: (DataFrame) single row df
        :param df: (DataFrame) df where the row is searched
        :return: Boolean
        """
        cols = df.columns
        bool_series = functools.reduce(lambda x, y: x & y, [df[col].isin(row[col]) for col in cols])
        return bool_series.any()

    def combo_appears_often_in_input(self, row, input_catego_df):
        """Given a combination of values for the columns in 'catego_cols' this function weights the amount
        of appearences in relation to the 'input_df' to determine if a filter excluding this combination is valuable.

        :param row: (DataFrame) single row df
        :param input_catego_df: (DataFrame): a DataFrame grouped by columns in 'catego_cols'
        :return: Boolean
        """
        df = input_catego_df.get_group(row)
        multi_col_filter_ratio = len(df) / len(self.input_df)
        if multi_col_filter_ratio > MULTI_COL_FILTER_RATIO:
            return 1
        else:
            return 0

    def determine_possible_multi_column_filters(self):
        """For the values / columns combinations generated using 'generate_possible_combinations', check if the
        combination exists in the 'input_df'.

        """
        combinations_generator = self.generate_possible_combinations()
        input_catego_df = self.input_df[self.catego_cols].drop_duplicates()
        for combo in combinations_generator:
            combo_row_df = pd.DataFrame([combo], columns=input_catego_df.columns)
            if self.isin_row(combo_row_df, input_catego_df):
                self.combos_to_check_in_final.append((combo_row_df, combo))

    def generate_possible_combinations(self):
        """For the columns in the 'catego_cols' list calculate all possible combinations of the columns unique values

        """
        lists_for_prod = list()
        if self.use_all_slicing_cols_as_catego_cols():
            self.catego_cols = self.slicing_cols
        else:
            self.set_catego_columns()
        logging.info("Category Columns:")
        logging.info(self.catego_cols)
        for col in self.catego_cols:
            unique_values = list(self.input_df[col].unique())
            lists_for_prod.append(unique_values)
        return itertools.product(*lists_for_prod)

    def determine_multi_column_filters(self):
        """For the values / columns combinations already generated and found in 'input_df' check that is exists in
        the 'final_df' and that it appears often, if so it is added to a DF for post in the report.

        """
        final_catego_df = self.final_df[self.catego_cols].drop_duplicates()
        input_catego_df = self.input_df.groupby(self.catego_cols)
        combos_to_exclude = list()
        for combo_row in self.combos_to_check_in_final:
            if (self.isin_row(combo_row[0], final_catego_df) and self.combo_appears_often_in_input(
                    combo_row[1], input_catego_df)):
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
        """Checks that the number of combinations to try as multiple column filter is less than:
        'MULTIPLE_COMBINATION_FILTERS' if all columns in 'slicing_cols' are considered. Because combinations
        grow quiet fast we need to be more careful.

        :return: Boolean
        """
        number_of_combinations = 1
        for col in self.slicing_cols:
            number_of_combinations *= len(self.input_df[col].unique())
            if number_of_combinations > MULTIPLE_COMBINATION_FILTERS:
                return 0
        return 1

    def set_catego_columns(self):
        """If the number of combinations is larger than 'MULTIPLE_COMBINATION_FILTERS' in take out the column with the
        must unique values and calculate the number of combinations again, until the number is bellow the threshold.

        :return:
        """
        slicing_cols_unique_length = dict()
        for col in self.slicing_cols:
            slicing_cols_unique_length.update({col: len(self.input_df[col].unique())})
        number_of_combinations = reduce(lambda x, y: x * y, slicing_cols_unique_length.values())
        while number_of_combinations > MULTIPLE_COMBINATION_FILTERS:
            max_key = max(slicing_cols_unique_length.items(), key=operator.itemgetter(1))[0]
            del slicing_cols_unique_length[max_key]
            number_of_combinations = reduce(lambda x, y: x * y, slicing_cols_unique_length.values())
        self.catego_cols = list(slicing_cols_unique_length.keys())

    def convert_str_col_to_date(self, col_name):
        """Tries to convert a column to dtype datetime64.

        :param col_name: (str) the name of the column
        :return: Boolean
        """
        try:
            self.input_df[col_name] = pd.to_datetime(self.input_df[col_name])
        except Exception:
            return 0
        return 1

    def find_largest_unused_catego_in_column(self, col):
        """Checks which value in a column in in 'input_df' that does not appear in 'final_df' appears the must
        amount of times.

        :param col: (str) the name of the column
        """
        unused_categos = set(self.input_df[col].unique()) - set(self.final_df[col].unique())
        category_potential = dict()
        for category in unused_categos:
            unused_rows_for_category = len(self.input_df.loc[self.input_df[col] == category])
            category_potential.update({category: unused_rows_for_category})
        return max(category_potential.items(), key=operator.itemgetter(1))[0]

    def determine_best_slicing_col_filter(self):
        """For the items in 'filtering_quick_gains' fin the one which yields the must benefit;
        two deciding factors: total number of rows that can be filtered (the larger the better),
        the number of elements to filter out (the larger the worse)

        """
        max_weighted_benefit = max([x['weighted_benefit'] for x in self.filtering_quick_gains])
        max_eficiency_potential_info = [item for item in self.filtering_quick_gains
                                        if item['weighted_benefit'] == max_weighted_benefit][0]
        if max_eficiency_potential_info['dtype'] != 'date':
            category_to_filter = self.find_largest_unused_catego_in_column(max_eficiency_potential_info['column'])
            logging.info("The suggested column to filter is {}, recommended value to filter: {}".format(
                max_eficiency_potential_info['column'], category_to_filter
            ))
            self.best_filter = (
                max_eficiency_potential_info['column'],
                category_to_filter,
                max_eficiency_potential_info['dtype'],
                max_eficiency_potential_info['excpeted_unused_rows_per_catego'],
            )
        else:
            logging.info("The suggested column to filter is {}, recommended period to filter: {}".format(
                max_eficiency_potential_info['column'], max_eficiency_potential_info['filter_out'][0]
            ))
            self.best_filter = (
                max_eficiency_potential_info['column'],
                max_eficiency_potential_info['filter_out'],
                max_eficiency_potential_info['dtype'],
                max_eficiency_potential_info['weighted_benefit'],
                len(self.input_df) / max_eficiency_potential_info['weighted_benefit']
            )
        logging.info("Full efficiency analisis: {}".format(self.filtering_quick_gains))
