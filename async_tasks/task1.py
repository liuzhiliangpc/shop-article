#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: task1.py
@time: 2021/4/22 15:26
@desc:
"""

import time
from init import celery

@celery.task
def add_together(a, b):
    print('%s + %s = %s' % (a, b, a+b))
    time.sleep(10)
    return a + b