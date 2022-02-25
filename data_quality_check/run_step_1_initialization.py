# -*- coding: utf-8 -*-
# @File       : run_step_1_initialization.py
# @Author     : Yuchen Chai
# @Date       : 2022-02-25 16:36
# @Description:

import os


if not os.path.exists("./store"):
    os.mkdir("./store")
if not os.path.exists("./output"):
    os.mkdir("./output")
