# Pandas-Report-Tracer
find filters that could be applied to your inputs to speed up the report calculation, for reports built using pandas

When creating a report we usually start with whole tables and start applying the business logic. After some back and forth with the data sources and the report stockholders we tend to wrap up our work by filtering the final columns, giving them more user friendly names and probably sorting the dataframe.

All that is very good, but must probably our report is not very efficient (takes more time than it could) because we are loading in memory more data than needed for the report. Loading unnecessary data takes loading time, memory and processing resources that are completely wasted. But given that the task of finding those filters is time consuming and given the fact that the report is already there, gives little incentive for the stakeholders to perfect it.

This is a WIP tool to automatize the task of finding the best filters for your input data, so your report runs faster using less memory. To give it a try:

Create a directory where to put this repo.
Clone the repo into your directory
run pip install -r requirements.txt
run python setup.py
Place an input file (csv) in /tmp/input/ and your resulting df file (csv) in /tmp/result/
run python pandas-report-tracer/main.py
check the report in /tmp/report_results_datetime.html
if you like what you see give it a try with python pandas-report-tracer/filter_one_input.py
