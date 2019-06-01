"""
clean your input DF so that it doesn't contain columns with different names but same information
"""
import datetime
import glob
import logging

import jinja2
import pandas as pd
from plotly.offline import plot
import plotly.graph_objs as go

from utils.columns_to_work_with import OneInputToFinalOptimization, analyze_one_input_to_result

INPUT_PATH = "/tmp/input"
RESULT_PATH = "/tmp/result"
MERGING_DICT = {
    "/tmp/input/committed_revisions_all.csv": ["revisionId"]
}
RENAMING_COLS_DICT = {
    "/tmp/input/example.csv": {"revision_id": "revisionId"}
}

def print_report(obj, input_file, result_file, data_usage_plot):
    templateLoader = jinja2.FileSystemLoader(searchpath="./report_templates/")
    templateEnv = jinja2.Environment(loader=templateLoader)
    TEMPLATE_FILE = "input_to_final.html"
    template = templateEnv.get_template(TEMPLATE_FILE)
    
    outputText = template.render(
        best_filter=obj.best_filter,
        input_files=[input_file],
        merging_cols=obj.merging_cols,
        matching_id_columns=obj.matching_id_cols,
        overall_percentage=obj.overall_percentage,
        data_usage_plot=data_usage_plot,
        filtering_quick_gains=obj.filtering_quick_gains,
        multi_columns_filter_df=obj.combos_to_exclude,
        result_file=result_file
    )
    html_file = open('/tmp/report_results.html', 'w')
    html_file.write(outputText)
    html_file.close()


if __name__ == "__main__":
    result_file = glob.glob(RESULT_PATH + "/*.csv")
    resulting_df = pd.read_csv(result_file[0], escapechar='\\')
    filenames = glob.glob(INPUT_PATH + "/*.csv")
    for csv_file in filenames:
        input_df = pd.read_csv(csv_file, escapechar='\\')
        if csv_file in RENAMING_COLS_DICT:
            input_df = input_df.rename(columns=RENAMING_COLS_DICT[csv_file])
        if csv_file in MERGING_DICT:
            x = OneInputToFinalOptimization(input_df, resulting_df, MERGING_DICT[csv_file])
        else:
            x = OneInputToFinalOptimization(input_df, resulting_df)
        analyze_one_input_to_result(x)

        # plotly horizontal bar chart for usage percentage:
        usage_data = [go.Bar(
            x=list(x.usage_percentage.values()),
            y=list(x.usage_percentage.keys()),
            orientation='h'
        )]
        data_usage_plot = plot(usage_data, auto_open=False, output_type='div')
        print_report(x, filenames[0], result_file[0], data_usage_plot)
