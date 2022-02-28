# -*- coding: utf-8 -*-
# @File       : run_step_2_generate_statistics.py
# @Author     : Yuchen Chai
# @Date       : 2022-02-25 16:32
# @Description:

from func_statistics import iterate_files
import sys
import argparse

# Step 1: Number of post statistics / sentiment trend
parser = argparse.ArgumentParser()
parser.add_argument('--year',
                    default=None,
                    help='Would you like to look at data from a specific year? If so, enter the year.')

args = parser.parse_args()
PARAMETER_YEAR = vars(args)['year']
iterate_files(PARAMETER_YEAR)

# Step 2: Missing files
