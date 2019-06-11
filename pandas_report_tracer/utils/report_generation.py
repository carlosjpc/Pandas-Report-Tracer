import datetime

import jinja2
from plotly.offline import plot
import plotly.graph_objs as go

TEMPLATE_FILE = "input_to_final.html"


def print_report(obj, input_file, result_file, data_usage_plot):
    template_loader = jinja2.FileSystemLoader(searchpath="./report_templates/")
    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template(TEMPLATE_FILE)

    output_text = template.render(
        best_filter=obj.best_filter,
        input_files=[input_file],
        merging_cols=obj.merging_cols,
        matching_id_columns=obj.matching_id_cols,
        overall_percentage=obj.overall_percentage,
        data_usage_plot=data_usage_plot,
        filtering_quick_gains=obj.filtering_quick_gains,
        multi_columns_filter_df=obj.combos_to_exclude.to_html(),
        result_file=result_file
    )
    now = datetime.datetime.now()
    html_file = open('/tmp/report_results_{}.html'.format(now.strftime("%d%m%Y_%H-%M-%S")), 'w')
    html_file.write(output_text)
    html_file.close()


def generate_data_usage_plot(obj):
    """Generates a plotly horizontal bar char on the axis 'x' and 'y'

    :param obj: (object) of the class OneInputToFinalOptimization
    :return: html plot
    """
    usage_data = [go.Bar(
        x=list(obj.usage_percentage.values()),
        y=list(obj.usage_percentage.keys()),
        orientation='h'
    )]
    return plot(usage_data, auto_open=False, output_type='div')
