#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: sync_update_es_increase_download.py
@time: 2021/4/25 18:32
@desc: download_count + 1操作，单次输入不超过10000条
和update_by_query二选一操作
"""

import time
from init import es, red_lock_factory

from retrying import retry
from pydantic import validate_arguments, ValidationError # 支持泛型参数校验
from typing import Any, Tuple, List, Dict # 泛型类型支持
import json
from core.core import logger


@validate_arguments # 类型不一致时自动强转变量类型
def validate_request(rowkey: str, seq_no: int, primary_term: int) -> bool:
    # 校验请求参数rowkey不能为空字符串
    if not rowkey:
        return False
    return True

# 拆为多个函数在web中执行
def run(request: Dict):
    """
    处理查询请求
    :param request: 请求参数
    :return: 请求结果
    """
    r = {}
    match_rowkey_version_list = []
    # 只校验必须字段，其他字段无法校验
    old_version_update_datas = request.get("data", [])
    # 检查校验
    for i, data in enumerate(old_version_update_datas):
        rowkey = data.get("rowkey")
        _seq_no = data.get("_seq_no")
        _primary_term = data.get("_primary_term")
        is_validated = False
        try:
            is_validated = validate_request(rowkey=rowkey, seq_no=_seq_no, primary_term=_primary_term)
        except ValidationError as e:
            logger.error("data validation failed:{}, {}".format(old_version_update_datas, e))
            r["retcode"] = 1
            r["msg"] = "some errors in arguments validation"
            logger.info("[消耗文章] [结果:{}]".format(str(r)))
            return r, match_rowkey_version_list

        if not is_validated: # rowkey为空情形亦跳出
            logger.error("rowkey is ''")
            r["retcode"] = 1
            r["msg"] = "some errors in arguments validation"
            logger.info("[消耗文章] [结果:{}]".format(str(r)))
            return r, match_rowkey_version_list

    indexs = "dw_article"
    # 校验版本
    match_rowkey_version_list, not_match_rowkey_version_list = verify_es_version(old_version_update_datas, indexs=indexs)
    # 若存在版本不一致的数据，则警告
    if not_match_rowkey_version_list:
        r["error_version_rowkey_list"] = not_match_rowkey_version_list # 一般情况该字段不存在，有问题才有
        r["retcode"] = 0
        r["msg"] = "partial success" # 部分运行成功
    else:
        r["retcode"] = 0
        r["msg"] = "success" # 完全执行成功，不考虑es基础服务正常返回

    if match_rowkey_version_list:
        match_rowkey_version_list_str = json.dumps(match_rowkey_version_list, ensure_ascii=False)
        try:
            with red_lock_factory.create_lock(f'update_es', ttl=120000):  # 加锁操作，120000毫秒/120秒失效
                status = update_by_query_es_data(query_id="update_by_query_increase_download_count", paras=[match_rowkey_version_list_str],
                                              indexs=indexs)
                if not status: # 传入基础服务失败
                    logger.warning("es service connect fail")
                    r["retcode"] = 1
                    r["msg"] = "es service error"
                    logger.info("[消耗文章] [结果:{}]".format(str(r)))
                    return r, match_rowkey_version_list
        except Exception as e:
            r["retcode"] = 1
            r["msg"] = "please try it again after one minute or repeat the query and update data later"  # 完全执行成功，不考虑es基础服务正常返回
            logger.info("[消耗文章] [结果:{},{}]".format(str(r), e))
            return r, match_rowkey_version_list
    logger.info("[文章消耗] [结果:{}]".format(str(r)))
    return r, match_rowkey_version_list


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
    status = es_back.get("success", False)
    return status


def get_batch_scroll_es_data(indexs: str, query_id: str, paras: List, scroll_id: str = ""):
    """
    游标方式获取ES数据库数据，提供文档，查询id是否已使用
    :param indexs: es 索引名
    :param query_id: es 模板
    :param paras: 参数列表
    :param scroll_id: 游标id 首次或中途、最后提交都会有scrollId这个参数
    :return:
    """
    if not scroll_id:  # 首次游标搜索
        res_scroll = es.search_scroll_pro(
            query_id=query_id, paras=paras, indexs=indexs, scroll=True
        )
    else:  # 之前有过游标id
        res_scroll = es.scroll(indexs=indexs, scroll_id=scroll_id)

    scroll_id = res_scroll.get("scrollId")  # 游标id
    es_response = res_scroll.get("restResponse")
    datas = []
    if es_response:
        count = es_response["hits"]["total"]["value"]
        if count > 0:
            datas = es_response["hits"]["hits"]  # 取匹配的若干条数据,最多为1000条数据

    return datas, scroll_id  # 返回本次数据和游标id


def get_required_fileds(es_datas: List) -> Dict:
    """
    解析es字段为需要的格式字段，包括rowkey、_seq_no、_primary_term。
    :param es_datas:
    :return: datas格式如：{'020058828560700da7544f5f1de51b': {'_seq_no': 118163, '_primary_term': 13}, '0200773838847028f290c8f76e44dd': {'_seq_no': 118164, '_primary_term': 13}}
    """
    datas = {}
    for i, data in enumerate(es_datas):
        rowkey = data["_source"].get("rowkey", "")
        _seq_no = data["_seq_no"]
        _primary_term = data["_primary_term"]
        datas[rowkey] = {"_seq_no": _seq_no, "_primary_term": _primary_term}
    return datas


def get_todo_download_datas(indexs: str, query_id: str, rowkey_list: List[str]) -> Dict:
    """
    根据rowkey_list获取数据，并解析es字段为需要的格式字段列表。
    :param indexs: es 索引名
    :param query_id: es 模板
    :param rowkey_list: rowkey列表
    :return: dw_datas_all格式如：{'020058828560700da7544f5f1de51b': {'_seq_no': 118163, '_primary_term': 13}, '0200773838847028f290c8f76e44dd': {'_seq_no': 118164, '_primary_term': 13}}
    """
    dw_datas_all = {}
    rowkey_list_str = json.dumps(rowkey_list, ensure_ascii=False)
    dw_datas, scroll_id = get_batch_scroll_es_data(
        indexs=indexs, paras=[rowkey_list_str], query_id=query_id
    )
    dw_datas_all.update(get_required_fileds(dw_datas)) # 汇总游标筛选的数据，筛选的未使用的这部分数据

    while 1:
        if dw_datas:
            dw_datas, scroll_id = get_batch_scroll_es_data(
                indexs=indexs,
                paras=[rowkey_list_str],
                query_id=query_id,
                scroll_id=scroll_id,
            )
            dw_mini_batch = get_required_fileds(dw_datas)  # 得到id字段
            dw_datas_all.update(dw_mini_batch)
        else:
            break
    logger.info("[消耗文章] [本批次应用库查到文档数为{}]".format(len(dw_datas_all)))
    return dw_datas_all


def verify_es_version(old_version_update_datas : List, indexs: str):
    """
    校验之前查询获得的版本和最新查询版本是否一致，一致则执行下一步更新字段；不一致则返回不一致的那几条数据（客户端重新取数据或跳过），其他数据继续执行更新。
    :param old_version_update_datas:
    :return:
    """
    rowkey_list = []
    for i, data in enumerate(old_version_update_datas):
        rowkey = data.get("rowkey", "")
        if rowkey:
            rowkey_list.append(rowkey)

    lastest_version_update_datas = get_todo_download_datas(indexs=indexs, query_id="query_version", rowkey_list=rowkey_list)
    # print(lastest_version_update_datas)

    not_match_rowkey_version_list = [] # 不一致的rowkey列表
    match_rowkey_version_list = [] # 一致的rowkey列表
    # 检查版本号差异
    for i, data in enumerate(old_version_update_datas):
        rowkey = data.get("rowkey")
        _seq_no = data.get("_seq_no")
        _primary_term = data.get("_primary_term")
        match_rowkey_version = lastest_version_update_datas.get(rowkey)
        if match_rowkey_version:
            if _seq_no == match_rowkey_version["_seq_no"] and _primary_term == match_rowkey_version["_primary_term"]:
                match_rowkey_version_list.append(rowkey)
            else:
                not_match_rowkey_version_list.append(rowkey)
    # logger.info("[消耗文章] [版本一致的rowkey:{}]".format(match_rowkey_version_list))
    logger.info("[消耗文章] [版本不一致的rowkey:{}]".format(not_match_rowkey_version_list))
    return match_rowkey_version_list, not_match_rowkey_version_list



if __name__ == "__main__":
    pass
    # 提交上次查询的数据rowkey和version，重新查询一次最新的version，若不一致，
    # 则返回不一致的那几条数据（客户端重新取数据或跳过），其他数据继续执行更新。

    # 前一步是同步结果
    # 更新阶段：
    # update_by_query 函数执行更新download_count字段
    # 这一步是异步
    indexs = "dw_article"
    request = {"data": [{"rowkey": "020058828560700da7544f5f1de51b", "_seq_no": 118165, "_primary_term": 13},
                        {"rowkey": "0200773838847028f290c8f76e44dd", "_seq_no": 118166, "_primary_term": 13,}]}
    # old_version_update_datas = request.get("data")
    # 不一致的rowkey需要返回告知请求方
    # verify_es_version(old_version_update_datas, indexs=indexs)
    print(run(request=request))



