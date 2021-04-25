#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: __init__.py
@time: 2021/4/22 18:28
@desc:
"""
# import os
# import sys
# dir_home = os.path.abspath(os.path.dirname(__file__))
# sys.path.append(os.path.join(dir_home, "../"))

from init import celery
from .task1 import add_together
from .sync_re_clean_pipe import run
