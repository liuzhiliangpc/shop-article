#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: sync_re_clean_pipe.py
@time: 2021/4/23 15:49
@desc:
"""

import time
from init import celery

from tools.log import logInit
from tools.baixing_elasticsearch import BXElasticSearch
from typing import Any, Tuple, List, Dict  # 泛型类型支持
from tools.milvus_utils101 import MyMilvus


mymilvus = MyMilvus()
es = BXElasticSearch()
logger = logInit("ElasticSearch && Milvus")
indexs = "dw_article"


@celery.task
def run(request: Dict) -> Dict:
    """
    校验请求功能
    :param request: 请求参数
    :return: 校验结果
    """
    r = {}
    # 获取一条请求数据
    re_clean_ids = request.get("id_list")  # 对应应用库中vector_id
    r.update({'res': sum(re_clean_ids)})
    init_milvus()
    return r

def init_milvus():
    pass



if __name__ == "__main__":
    time0 = time.time()
    data = {"id_list": [395153, 459730]}
    print(run(request=data))
    # 顺序为先加锁，并根据id范围查询未使用的rowkey，然后异步删除对应的向量和es文档，完成后解锁退出
    # 缺少的异步环节是确认成功删除的查询，es和向量都要检查。
    # 可视为查询和删除、再确认删除成功是一个调度任务。
    print(time.time()- time0)