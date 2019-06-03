import datetime

import jinja2
from plotly.offline import plot
import plotly.graph_objs as go


def print_report(obj, input_file, result_file, data_usage_plot):
    template_loader = jinja2.FileSystemLoader(searchpath="./report_templates/")
    template_env = jinja2.Environment(loader=template_loader)
    TEMPLATE_FILE = "input_to_final.html"
    template = template_env.get_template(TEMPLATE_FILE)
    
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
    now = datetime.datetime.now()
    html_file = open('/tmp/report_results_{}.html'.format(now.strftime("%d%m%Y_%H-%M-%S")), 'w')
    html_file.write(outputText)
    html_file.close()


def generate_data_usage_plot(x):
    # plotly horizontal bar chart for usage percentage:
    usage_data = [go.Bar(
        x=list(x.usage_percentage.values()),
        y=list(x.usage_percentage.keys()),
        orientation='h'
    )]
    return plot(usage_data, auto_open=False, output_type='div')
