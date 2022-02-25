# -*- coding: utf-8 -*-
# @File       : run_step_2_generate_statistics.py
# @Author     : Yuchen Chai
# @Date       : 2022-02-25 16:32
# @Description:

from data_quality_check.func_statistics import iterate_files

# Step 1: Number of post statistics / sentiment trend
PARAMETER_YEAR = None
iterate_files(PARAMETER_YEAR)

# Step 2: Missing files
