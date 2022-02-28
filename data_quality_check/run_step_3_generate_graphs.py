# -*- coding: utf-8 -*-
# @File       : run_step_3_generate_graphs.py
# @Author     : Yuchen Chai
# @Date       : 2022-02-25 16:38
# @Description:

import sys
from func_plotting import iterate_plotting

PARAMETER_YEAR = sys.argv[1]
print(PARAMETER_YEAR)

PARAMETER_AREA_LEVEL = sys.argv[2]

PARAMETER_AREA = sys.argv[3]

iterate_plotting(PARAMETER_YEAR, PARAMETER_AREA_LEVEL, PARAMETER_AREA)
