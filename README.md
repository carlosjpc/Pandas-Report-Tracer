# Pandas-Report-Tracer
[![Build Status](https://travis-ci.com/carlosjpc/Pandas-Report-Tracer.png?branch=master)](https://travis-ci.com/carlosjpc/Pandas-Report-Tracer) [![codecov](https://codecov.io/gh/carlosjpc/Pandas-Report-Tracer/branch/master/graph/badge.svg)](https://codecov.io/gh/carlosjpc/Pandas-Report-Tracer)

Predicate Push Down (PPD) rely on explicit filters. This tool finds implicit filters, coming from joins or merges that could be applied to your inputs to speed up the data mart calculation.

This is an exploratory tool to automatize the task of finding the best filters for your input data, so your etl and report runs faster using less memory. To give it a try:

- Create a directory where to put this repo.
- Clone the repo into your directory
- run pip install -r requirements.txt
- run python setup.py
- Place an input file (csv) in /tmp/input/ and your resulting df file (csv) in /tmp/result/
- run python pandas-report-tracer/main.py
- check the report in /tmp/report_results_datetime.html
- if you like what you see give it a try with python pandas-report-tracer/filter_one_input.py
