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

from init import celery
from .task1 import add_together
from .task3 import run
from .sync_check_es import sync_check_es_update
from .sync_check_es import sync_check_clean