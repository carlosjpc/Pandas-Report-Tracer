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


class SingleColumnFilters:
    date_slices = {
        'one_month_period': TODAY - datetime.timedelta(days=31),
        'one_quarter_period': TODAY - datetime.timedelta(days=92),
        'six_months_period': TODAY - datetime.timedelta(days=183),
        'one_year_period': TODAY - datetime.timedelta(days=365),
        'mtd': datetime.date(TODAY.year, TODAY.month, 1),
        'ytd': datetime.date(TODAY.year, 1, 1)
    }

    def __init__(self, input_df, resulting_df, merging_cols=None, input_file_name=None):
        self.extended_resulting_df = pd.DataFrame()
        self.filtering_quick_gains = list()
        self.final_df = pd.DataFrame()
        self.input_df = input_df
        self.input_file_name = input_file_name
        self.matching_cols = list()
        self.matching_id_cols = list()
        self.merging_cols = merging_cols
        self.natural_dividers_dtypes = dict()
        self.overall_percentage = float()
        self.resulting_df = resulting_df
        self.usage_percentage = dict()

    def make_analysis(self, apply_filter=False):
        """Runs functions of the class SingleColumnFilters on an object of the class to get the best single
        column filter while also generating the data for the 'print_report' and 'generate_data_usage_plot'.

        Parameters:
        self (object): an object of the class SingleColumnFilters
        Returns:
        None
        """
        logging.info("Starting Analisis")
        self.find_matching_cols()
        if not self.matching_cols:
            logging.warning('Without shared columns this tool is worthless, consider renaming columns')
            return
        self._merge_input_to_final()
        self._set_final_df_to_work_with()
        self.columns_usage_percentage()
        logging.info("Column usage percentage:")
        logging.info(self.usage_percentage)
        self.get_dtypes_for_natural_divider_cols()
        for slicing_col, dtype in self.natural_dividers_dtypes.items():
            if 'date' in dtype:
                self._determine_date_range_filters(slicing_col)
            else:
                self._determine_category_col_filters(slicing_col)
        self._determine_best_slicing_col_filter()
        if apply_filter:
            self._filter_and_save_inputfile(self.input_file_name)

    def find_matching_cols(self):
        """Compares the columns in 'input_df' and 'resulting_df' to find columns present in both DF, a second list is
        calculated for columns also having id or Id in their name, to be used for merging the DFs if no 'merging_cols'
        list is provided.

        """
        self.matching_cols = list(set(self.input_df.columns).intersection(self.resulting_df.columns))
        self.matching_id_cols = [col_name for col_name in self.matching_cols if 'id' in col_name or 'Id' in col_name]

    def _merge_input_to_final(self):
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

    def _set_final_df_to_work_with(self):
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

    def get_dtypes_for_natural_divider_cols(self):
        """For each column in 'input_df' determine its dtype (many columns have dtype object which is not useful
        for this analysis). And add to a dictionary if 'is_natural_divider'

        """
        for col in self.input_df.columns:
            if 'date' in col or 'Date' in col:
                self.input_df[col] = pd.to_datetime(self.input_df[col])
                self.natural_dividers_dtypes.update({col: 'date'})
            elif is_string_dtype(self.input_df[col]):
                if self._is_natural_divider(self.input_df[col]):
                    self.natural_dividers_dtypes.update({col: 'string'})
            elif is_integer_dtype(self.input_df[col]):
                if self._is_natural_divider(self.input_df[col]):
                    self.natural_dividers_dtypes.update({col: 'integer'})
            elif is_bool_dtype(self.input_df[col]):
                self.natural_dividers_dtypes.update({col: 'boolean'})

    def _is_natural_divider(self, df_series):
        """Determines if a column of the 'input_df' would serve as a good filter, if the ratio of unique values to
        the number of rows is high the column is consider a 'natural divider' date columns are always natural dividers,
        but this function they may not show as, be cautious.

        :param df_series: (pandas series) series to be checked to see if it is a 'natural divider'
        :return: Boolean
        """
        unique_rows = len(df_series.unique())
        if unique_rows == 1:
            return
        total_rows = len(df_series)
        ratio = total_rows / unique_rows
        logging.info('Column: {} has {} unique rows in {} rows, a {} to 1 relationship'.format(
            df_series._name, str(unique_rows), str(total_rows), str(ratio)))
        if ratio > NATURAL_DIVIDER_THRESOLD and self.usage_percentage[df_series._name] != 1:
            return 1

    def _handle_na_in_date_cols(self, col):
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

    def _determine_date_range_filters(self, col):
        """For each of the time ranges determined in the 'date_slices' class dictionary this functions checks if
        there is data in the 'final_df' if not it continues with the next date range, otherwise it checks if there is
        data for that same date range in the 'input_df', if so, there is un needed rows in the 'input_df' that should
        be filtered. That information is stored in function variables to later, determine the best date range filter
        that can be applied to that column, that information is stored in a dictionary and appended to
        'filtering_quick_gains' an instance list.

        :param col: (str) the name of the column in the 'input_df'
        """
        self._handle_na_in_date_cols(col)
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
                    weighted_benefit = useless_rows = len(lesser_date_input)
                    the_column = col
                    filter_out = (period, date_)
        if weighted_benefit > 0:
            self.filtering_quick_gains.append({
                'column': the_column,
                'dtype': 'date',
                'filter_out': filter_out,
                'useless_rows': useless_rows,
                'weighted_benefit': weighted_benefit
            })

    def _determine_category_col_filters(self, col):
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
            'dtype': self.natural_dividers_dtypes[col],
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

    def find_largest_unused_catego_in_column(self, col):
        """Checks which value in a column in in 'input_df' that does not appear in 'final_df' appears the must
        amount of times.

        :param col: (str) the name of the column
        """
        unused_categos = set(self.input_df[col].unique()) - set(self.final_df[col].unique())
        if not unused_categos:
            raise ValueError("This column has no unused categories")
        category_potential = dict()
        for category in unused_categos:
            unused_rows_for_category = len(self.input_df.loc[self.input_df[col] == category])
            category_potential.update({category: unused_rows_for_category})
        return max(category_potential.items(), key=operator.itemgetter(1))[0]

    def _determine_best_slicing_col_filter(self):
        """For the items in 'filtering_quick_gains' fin the one which yields the must benefit;
        two deciding factors: total number of rows that can be filtered (the larger the better),
        the number of elements to filter out (the larger the worse)

        """
        self.max_weighted_benefit = max([x['weighted_benefit'] for x in self.filtering_quick_gains])
        max_eficiency_potential_info = [item for item in self.filtering_quick_gains
                                        if item['weighted_benefit'] == self.max_weighted_benefit][0]
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
                max_eficiency_potential_info['weighted_benefit'] / len(self.input_df)
            )

    def _filter_and_save_inputfile(self, input_file_name):
        """Applies the best filter, result of running 'analyze_one_input_to_result' to the input_df and saves it,
        replacing the original file.

        Parameters:
        input_file_name (string): the path and name of the input csv
        Returns:
        None
        """
        # filter input DF with best filter found in analisis
        if self.best_filter[2] == 'date':
            nan_rows = self.input_df.loc[self.input_df[self.best_filter[0]].isnull()]
            self.input_df = self.input_df.loc[
                self.input_df[self.best_filter[0]] > self.best_filter[1][1]
            ]
            self.input_df = pd.concat([self.input_df, nan_rows])
        else:
            self.input_df = self.input_df.loc[
                self.input_df[self.best_filter[0]] != self.best_filter[1]
            ]
        self.input_df.to_csv(input_file_name, index=False, encoding='utf-8', escapechar='\\')


class MultiColumnFilters(SingleColumnFilters):

    def __init__(self, input_df, resulting_df, merging_cols=None):
        self.input_df = input_df
        self.resulting_df = resulting_df
        self.merging_cols = merging_cols
        super(SingleColumnFilters, self).__init__()
        self.usage_percentage = dict()
        self.combo_cols = dict()
        self.combos_to_check_in_final = list()
        self.combos_to_exclude = pd.DataFrame()

    def get_multi_column_filters(self):
        logging.info("Starting Multi Column Filter Analisis")
        self.find_matching_cols()
        if not self.matching_cols:
            logging.warning('Without shared columns this tool is worthless, consider renaming columns')
            return
        self._merge_input_to_final()
        self._set_final_df_to_work_with()
        self.columns_usage_percentage()
        self._determine_possible_multi_column_filters()
        self._determine_multi_column_filters()

    def _combo_appears_often_in_input(self, combo, input_catego_groupby):
        """Given a combination of values for the columns in 'combo_cols' this function weights the amount
        of appearences in relation to the 'input_df' to determine if a filter excluding this combination is valuable.

        :param row: (DataFrame) single row df
        :param input_catego_df: (DataFrame): a DataFrame grouped by columns in 'combo_cols'
        :return: Boolean
        """
        df = self.input_df.loc[input_catego_groupby.groups[combo]]
        multi_col_filter_ratio = len(df) / len(self.input_df)
        if multi_col_filter_ratio > MULTI_COL_FILTER_RATIO:
            return 1
        else:
            return 0

    def _determine_possible_multi_column_filters(self):
        """For the values / columns combinations generated using 'generate_possible_combinations', check if the
        combination exists in the 'input_df'.

        """
        combinations_generator = self._generate_possible_combinations()
        input_catego_groupby = self.input_df.groupby(self.combo_cols)
        for combo in combinations_generator:
            if combo in input_catego_groupby.groups:
                if self._combo_appears_often_in_input(combo, input_catego_groupby):
                    combo_row_df = pd.DataFrame([combo], columns=self.combo_cols)
                    self.combos_to_check_in_final.append(combo_row_df)

    def _generate_possible_combinations(self):
        """For the columns in the 'combo_cols' list calculate all possible combinations of the columns unique values

        """
        lists_for_prod = list()
        self._set_combo_columns()
        for col in self.combo_cols:
            unique_values = list(self.input_df[col].unique())
            lists_for_prod.append(unique_values)
        return itertools.product(*lists_for_prod)

    def _determine_multi_column_filters(self):
        """For the values / columns combinations already generated and found in 'input_df' check that is exists in
        the 'final_df' and that it appears often, if so it is added to a DF for post in the report.

        """
        resulting_catego_df = self.final_df[self.combo_cols].drop_duplicates()
        combos_to_exclude = list()
        for combo_row in self.combos_to_check_in_final:
            if isin_row(combo_row, resulting_catego_df):
                combos_to_exclude.append(combo_row)
        self.combos_to_exclude = pd.concat(combos_to_exclude)

    def _set_combo_columns(self):
        """If the number of combinations is larger than 'MULTIPLE_COMBINATION_FILTERS' in take out the column with the
        must unique values and calculate the number of combinations again, until the number is bellow the threshold.

        :return:
        """
        for col in self.input_df.columns:
            if self._is_natural_divider(self.input_df[col]):
                self.combo_cols.update({col: len(self.input_df[col].unique())})
        number_of_combinations = reduce(lambda x, y: x * y, self.combo_cols.values())
        while number_of_combinations > MULTIPLE_COMBINATION_FILTERS:
            max_key = max(self.combo_cols.items(), key=operator.itemgetter(1))[0]
            del self.combo_cols[max_key]
            number_of_combinations = reduce(lambda x, y: x * y, self.combo_cols.values())
        self.combo_cols = list(self.combo_cols.keys())
