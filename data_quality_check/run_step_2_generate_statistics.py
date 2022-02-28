# -*- coding: utf-8 -*-
# @File       : run_step_2_generate_statistics.py
# @Author     : Yuchen Chai
# @Date       : 2022-02-25 16:32
# @Description:

from func_statistics import iterate_files
import sys

# Step 1: Number of post statistics / sentiment trend
PARAMETER_YEAR = sys.argv[1]
print(sys.argv[1])
iterate_files(PARAMETER_YEAR)

# Step 2: Missing files
