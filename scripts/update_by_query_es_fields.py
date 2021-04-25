#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: update_es_fields.py
@time: 2021/4/19 10:51
@desc: 修改应用库通用接口，包括更新download次数、修改status、is_used状态,已修改为仅用rowkey更新es，这个比较快
"""

from tools.log import logInit
# from tools.baixing_elasticsearch import BXElasticSearch
from retrying import retry
from pydantic import validate_arguments, ValidationError # 支持泛型参数校验
from typing import Any, Tuple, List, Dict # 泛型类型支持
import json
import copy
from init import es

# es = BXElasticSearch()
logger = logInit("ElasticSearch")

def update_by_query_es_data(query_id: str, paras: List, indexs: str = "dw_article") -> Dict:
    """
    查询更新，批量接口，支持重试
    :param query_id: es模板
    :param paras: 参数列表
    :param indexs: 索引名
    :return:
    """
    @retry(stop_max_attempt_number=3)
    def es_retry():
        es_back = es.update_by_query_pro(query_id=query_id, paras=paras, indexs=indexs)
        return es_back
    es_back = {}
    try:
        es_back = es_retry()
    except Exception as e:
        logger.exception(msg=e)
    # print(es_back)
    return es_back

@validate_arguments # 类型不一致时自动强转变量类型
def validate_request(rowkey_list: List[str], vector_id_list: List[str], option: str) -> bool:
    # 校验请求参数rowkey_list不能为空值
    if option in ("increase_download_count", "reduce_download_count", "ban", "re_clean"):
        if not rowkey_list:
            return False
    return True

def run(request):
    """
    处理查询请求
    :param request: 请求参数
    :return: 请求结果
    """
    r = {}
    # 获取一条请求数据
    # 只校验必须字段，其他字段无法校验
    option = request.get("option", "increase_download_count")
    rowkey_list = request.get("rowkey_list", [])
    vector_id_list = request.get("vector_id_list", [])

    is_validated = False
    try:
        is_validated = validate_request(rowkey_list=rowkey_list, vector_id_list=vector_id_list, option=option)
    except ValidationError as e:
        logger.error("data validation failed: {}".format(e))
        r["retcode"] = 1
        r["msg"] = "some errors in arguments validation"
        return r

    if not is_validated:
        logger.error("data is []")
        r["retcode"] = 1
        r["msg"] = "some errors in arguments validation"
        return r

    indexs = "dw_article"
    if option in ("increase_download_count", "reduce_download_count", "ban","re_clean"):
        rowkey_list_str = json.dumps(rowkey_list, ensure_ascii=False)

    if option == "increase_download_count":
        es_back = update_by_query_es_data(query_id="update_by_query_increase_download_count", paras=[rowkey_list_str], indexs=indexs)
    elif option == "reduce_download_count":
        es_back = update_by_query_es_data(query_id="update_by_query_reduce_download_count", paras=[rowkey_list_str], indexs=indexs)
    elif option == "ban":
        es_back = update_by_query_es_data(query_id="update_by_query_to_ban", paras=[rowkey_list_str], indexs=indexs)
    elif option == "re_clean":
        es_back = update_by_query_es_data(query_id="update_by_query_re_clean", paras=[rowkey_list_str], indexs=indexs)
    # 缺少es基础服务的异步返回
    r = {
        "retcode": 0,
        "msg": "success",
    }
    logger.info("[更新请求] [结果:{}]".format(str(r)))
    return r


if __name__ == "__main__":

    request = {
        "rowkey_list": ["020058828560700da7544f5f1de51b", "0200773838847028f290c8f76e44dd"],
        "option": "increase_download_count",
    }

    run(request)

