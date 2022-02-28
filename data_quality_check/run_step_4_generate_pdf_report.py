# -*- coding: utf-8 -*-
# @File       : run_step_4_generate_pdf_report.py
# @Author     : Yuchen Chai
# @Date       : 2022-02-25 16:39
# @Description:

from func_pdf import generate_report

PARAMETER_YEAR = None

# Step 1: generate public pdf report
generate_report([2012], ["World"])
    
# Step 2: generate internal pdf report
