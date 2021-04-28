#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: sync_update_es_fields.py
@time: 2021/4/25 10:51
@desc: 修改应用库通用接口，包括更新download次数、修改status、is_used状态,已修改为仅用rowkey更新es，这个比较快
"""
import time
from tools.log import logInit

from retrying import retry
from pydantic import validate_arguments, ValidationError # 支持泛型参数校验
from typing import Any, Tuple, List, Dict # 泛型类型支持
import json
from init import celery, es, red_lock_factory


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
    status = es_back.get("success", False)
    return status

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
def validate_request(rowkey_list: List[str], vector_id_list: List[str], option: str) -> bool:
    # 校验请求参数rowkey_list不能为空值
    if option in ("increase_download_count", "reduce_download_count", "ban", "re_clean"):
        if not rowkey_list:
            return False
    return True

@celery.task
def run(request: Dict) -> Dict:
    """
    处理查询请求
    :param request: 请求参数
    :return: 请求结果
    """
    r = {}
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
        logger.info("[更新请求] [结果:{}]".format(str(r)))
        return r

    if not is_validated:
        logger.error("data is []")
        r["retcode"] = 1
        r["msg"] = "some errors in arguments validation"
        logger.info("[更新请求] [结果:{}]".format(str(r)))
        return r

    indexs = "dw_article"
    if option in ("increase_download_count", "reduce_download_count", "ban","re_clean"):
        rowkey_list_str = json.dumps(rowkey_list, ensure_ascii=False)
    else:
        logger.error("{} option not support".format(option))
        r["retcode"] = 1
        r["msg"] = "option not support"
        logger.info("[更新请求] [结果:{}]".format(str(r)))

    with red_lock_factory.create_lock(f'update_es', ttl=120000):  # 加锁操作，120000毫秒/120秒失效

        if option == "increase_download_count":
            status = update_by_query_es_data(query_id="update_by_query_increase_download_count", paras=[rowkey_list_str], indexs=indexs)
        elif option == "reduce_download_count":
            status = update_by_query_es_data(query_id="update_by_query_reduce_download_count", paras=[rowkey_list_str], indexs=indexs)
        elif option == "ban":
            status = update_by_query_es_data(query_id="update_by_query_to_ban", paras=[rowkey_list_str], indexs=indexs)
        elif option == "re_clean":
            status = update_by_query_es_data(query_id="update_by_query_re_clean", paras=[rowkey_list_str], indexs=indexs)
        if not status: # 传入基础服务失败
            logger.warning("es service connect fail")
            r["retcode"] = 1
            r["msg"] = "es service error"
            logger.info("[更新请求] [结果:{}]".format(str(r)))
            return r
        # 检查es操作是否完成，后续可接es基础服务的异步结果，目前不支持
        longest_time = 60  # 允许基础服务积压60秒处理
        check_query_id_dict = {"increase_download_count": "query_check_increase_download_count",
                               "reduce_download_count": "query_check_reduce_download_count",
                               "ban": "query_check_to_ban",
                               "re_clean": "query_check_re_clean", }
        # 需要说明的是，当限制最大使用次数不等于1时，检查操作完成要跳过。
        # increase_download_count和reduce_download_count两个需要es基础服务的返回才有效
        while longest_time > 0:
            time.sleep(1)
            longest_time -= 1
            check_dw_datas = get_batch_es_data(query_id=check_query_id_dict.get(option), paras=[rowkey_list_str],
                                               indexs=indexs)
            if len(check_dw_datas) == 0:
                break
    r["retcode"] = 0
    r["msg"] = "success"  # 完全执行成功，不考虑es基础服务正常返回
    logger.info("[更新请求] [结果:{}]".format(str(r)))
    return r


if __name__ == "__main__":

    request = {
        "rowkey_list": ["020058828560700da7544f5f1de51b", "0200773838847028f290c8f76e44dd"],
        # "option": "increase_download_count",
        "option": "reduce_download_count",
    }

    run(request)

