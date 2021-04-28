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

from pydantic import validate_arguments, ValidationError  # 支持泛型参数校验
from typing import Any, Tuple, List, Dict  # 泛型类型支持
from core.core import logger


def run(request: Dict) -> Dict:
    """
    校验请求功能
    :param request: 请求参数
    :return: 校验结果
    """
    r = {}
    # 获取一条请求数据
    re_clean_ids = request.get("id_list")  # 对应应用库中vector_id
    is_validated = False
    try:
        is_validated = validate_request(re_clean_ids=re_clean_ids)
    except ValidationError as e:
        logger.error("data validation failed:{},{}".format(re_clean_ids, e))
        r["retcode"] = 1
        r["msg"] = "some errors in arguments validation"
        logger.info("[二次更新] [结果:{}]".format(str(r)))
        return r
    if not is_validated: # re_clean_ids为空情形亦跳出
        logger.error("data is none:{}".format(re_clean_ids))
        # raise ValueError("data is none:{}".format(re_clean_ids))
        r["retcode"] = 1
        r["msg"] = "data validation error"
        logger.info("[二次更新] [结果:{}]".format(str(r)))
        return r
    r["retcode"] = 0
    r["msg"] = "success"
    logger.info("[二次清洗] [结果:{}]".format(str(r)))
    return r


@validate_arguments
def validate_request(re_clean_ids: List[int]) -> bool:
    # 校验request请求参数是否为空值
    if not re_clean_ids:
        return False
    for element in re_clean_ids:
        if not element:
            return False
    return True


if __name__ == "__main__":
    time0 = time.time()
    data = {"id_list": [395153, 459730]}
    print(run(request=data))
    # 顺序为先加锁，并根据id范围查询未使用的rowkey，然后异步删除对应的向量和es文档，完成后解锁退出
    # 缺少的异步环节是确认成功删除的查询，es和向量都要检查。
    # 可视为查询和删除、再确认删除成功是一个调度任务。
    print(time.time()- time0)