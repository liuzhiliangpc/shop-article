#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: query.py
@time: 2021/4/16 10:57
@desc: ES查询接口
"""

from retrying import retry
from pydantic import validate_arguments, ValidationError # 支持泛型参数校验
from typing import Any, Tuple, List, Dict # 泛型类型支持
import json
import copy
from init import es
from core.core import logger


def get_batch_es_data(query_id: str, paras: List, indexs: str = "dw_article", max_nums: int = 10000) -> List:
    """
    获取ES数据库数据，提供文档
    :param indexs: es 索引名
    :param query_id: es 模板
    :param max_nums: 前nums数据
    :param paras: 参数列表
    :return:
    """
    @retry(stop_max_attempt_number=3)
    def es_retry():
        es_back = es.search_pro(query_id=query_id, paras=paras, indexs=indexs)
        return es_back

    es_back = {}
    try:
        es_back = es_retry()
    except Exception as e:
        logger.exception(msg=e)
    es_response = es_back.get("restResponse")
    datas = []
    if es_response:
        count = es_response["hits"]["total"]["value"]
        if count > 0:
            datas = es_response["hits"]["hits"][:max_nums]  # 取匹配的若干条数据,最多为10000条数据
    return datas

@validate_arguments # 类型不一致时自动强转变量类型
def validate_request(root_A: str, root_B: str, root_C: str, root_D: str, business_category_list: List[str],
                     no_rowkey_list: List[str], no_vector_id_list: List[str], size: int) -> bool:
    # 校验请求参数root_C不能为空值
    if not root_C:
        return False
    return True

def get_data_match_title(title_string_rule: str, query_id: str, indexs: str="dw_article", download_count_lte: int=0, business_category_list: List[str]=["B2B, B2C"],
                         size: int=1000, params: Dict={}) -> List:
    """获取标题匹配数据，支持额外加一些term项，但限制了任意query查询。返回仅包括必须的字段"""
    no_rowkey_list = params.get("no_rowkey_list", [])
    no_vector_id_list = params.get("no_vector_id_list", [])
    no_rowkey_list_str = json.dumps(no_rowkey_list, ensure_ascii=False)
    no_vector_id_list_str = json.dumps(no_vector_id_list, ensure_ascii=False)
    business_category_list_str = json.dumps(business_category_list, ensure_ascii=False)

    default_keys = ["no_rowkey_list", "no_vector_id_list"] # 针对性排除两个排除字段，其他字段是真正的额外字段
    other_params = copy.deepcopy(params)  # 其他的字段
    for key in default_keys:
        other_params.pop(key)
    other_term_str = ""
    for key, value in other_params.items():
        other_term_str = other_term_str + """{"term": {"%s": "%s"}},""" % (key, value) # other = """{"term": {"industry_l2": "%s"}},""" % (industry_l2)
    es_datas = get_batch_es_data(indexs=indexs, query_id=query_id,
                             paras=[title_string_rule, other_term_str, download_count_lte, business_category_list_str,
                                    no_rowkey_list_str, no_vector_id_list_str, size])
    # 解析es字段为需要的格式字段，具体的版本号字段，下游要使用更新接口时，必须带版本号。
    datas = []
    for i, data in enumerate(es_datas):
        element = {}
        element["_seq_no"] = data["_seq_no"]
        element["_primary_term"] = data["_primary_term"]
        element["rowkey"] = data["_source"]["rowkey"]
        element["title"] = data["_source"].get("title", "")
        element["content"] = data["_source"].get("content", "")
        datas.append(element)
    return datas

def run(request):
    """
    处理查询请求
    :param request: 请求参数
    :return: 请求结果
    """
    r = {}
    # 获取一条请求数据
    # 只校验必须字段，其他字段无法校验
    business_category_list = request.get("business_category_list", ["B2B", "B2C"])
    root_A = request.get("root_A", "")
    root_B = request.get("root_B", "")
    root_C = request.get("root_C", "")
    root_D = request.get("root_D", "")
    no_rowkey_list = request.get("no_rowkey_list", [])
    no_vector_id_list = request.get("no_vector_id_list", [])
    size = request.get("size", 10)
    size = min(size, 10000) # 游标方式不支持相关度排序
    is_validated = False
    try:
        is_validated = validate_request(root_A=root_A, root_B=root_B, root_C=root_C, root_D=root_D, business_category_list=business_category_list,
                     no_rowkey_list=no_rowkey_list, no_vector_id_list=no_vector_id_list, size=size)
    except ValidationError as e:
        logger.error("data validation failed: {}".format(e))
        r["retcode"] = 1
        r["msg"] = "some errors in arguments validation"
        return r

    if not is_validated:
        logger.error("root_C is ''")
        r["retcode"] = 1
        r["msg"] = "some errors in arguments validation"
        return r

    indexs = "dw_article"
    query_id = "query_main"
    download_count_lte = 0 # 最大使用次数

    root_list = [root_A, root_B, root_C, root_D]
    check_root_list = []
    for root_i in root_list:
        if root_i:
          check_root_list.append(root_i)
    title_string_rule = "&&".join(check_root_list)

    default_keys = ["index", "root_A", "root_B", "root_C", "root_D",
        "business_category_list", "size"]
    request_kwargs = copy.deepcopy(request)
    for key in default_keys:
        if key in request: # 默认字段在输入才弹出
            request_kwargs.pop(key)  # request_kwargs = {"industry_l2": "律师服务", "industry_l1": "服务"}

    datas_from_elasticsearch = get_data_match_title(title_string_rule=title_string_rule, query_id=query_id, indexs=indexs,
                         download_count_lte=download_count_lte, business_category_list=business_category_list,
                         size=size, params=request_kwargs)
    r = {
        "retcode": 0,
        "data": datas_from_elasticsearch,
        "msg": "success",
    }
    logger.info("[查询请求] [结果:{}]".format(str(r)))
    return r


if __name__ == "__main__":

    request = {
        "root_A": "",
        "root_B": "",
        "root_C": "律师",
        "root_D": "",
        "business_category_list": ["B2B", "B2C"],
        "no_rowkey_list": ["1"],
        "no_vector_id_list": ["1"],
        "size": 1,
        "industry_l2": "律师服务",
        "industry_l1": "服务",
    }

    run(request)
