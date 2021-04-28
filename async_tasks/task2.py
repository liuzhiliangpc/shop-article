#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: task2.py
@time: 2021/4/25 11:40
@desc:
"""

import time
from init import celery

from tools.log import logInit
from retrying import retry
from typing import Any, Tuple, List, Dict  # 泛型类型支持
import json
from tools.milvus_utils101 import MyMilvus
import concurrent.futures
import numpy as np
from pydantic import validate_arguments, ValidationError # 支持泛型参数校验


logger = logInit("ElasticSearch && Milvus")
indexs = "dw_article"


from elasticsearch import Elasticsearch
es = Elasticsearch(hosts="172.30.1.98:9200")


@celery.task
def sync_re_clean(request: Dict) -> Dict:
    """
    校验请求功能
    :param request: 请求参数
    :return: 校验结果
    """
    r = {}
    # 获取一条请求数据
    re_clean_ids = request.get("id_list")  # 对应应用库中vector_id
    r.update({'res': sum(re_clean_ids)})
    return r

