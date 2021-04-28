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
from init import celery, es

from init import mymilvus, red_lock_factory

from retrying import retry
from pydantic import validate_arguments, ValidationError  # 支持泛型参数校验
from typing import Any, Tuple, List, Dict # 泛型类型支持
import json
import concurrent.futures
from core.core import logger


check_query_id_dict = {"increase_download_count": "query_check_increase_download_count",
                       "reduce_download_count": "query_check_reduce_download_count",
                       "ban": "query_check_to_ban",
                       "re_clean": "query_check_re_clean",}

@celery.task
def sync_check_es_update(rowkey_list: List[str], option: str, indexs: str = "dw_article", longest_time: int = 120) -> bool:
    # 检查es操作是否完成，后续可接es基础服务的异步结果，目前不支持
    # 默认允许基础服务积压120秒处理
    # 需要说明的是，当限制最大使用次数不等于1时，检查操作完成要跳过。
    # increase_download_count和reduce_download_count两个需要es基础服务的返回才有效
    rowkey_list_str = json.dumps(rowkey_list, ensure_ascii=False)
    while longest_time > 0:
        time.sleep(1)
        longest_time -= 1
        check_dw_datas = get_batch_es_data(query_id=check_query_id_dict.get(option), paras=[rowkey_list_str],
                                           indexs=indexs)
        if len(check_dw_datas) == 0:
            return True # 执行成功
    return False # 超时执行失败，但实际不一定失败，es基础服务的结果，由于采用消息队列，一般不会丢，但积压可能产生版本异常


@celery.task
def sync_check_clean(vector_id_list: List[int], indexs: str = "dw_article", longest_time: int = 120) -> bool:
    # 加锁操作,整体放入异步
    @retry(wait_exponential_multiplier = 1000, wait_exponential_max = 10000)
    def save_modify(vector_id_list, indexs, longest_time): # 中间修改了longest变量，longest必须传参
        with red_lock_factory.create_lock(f'update_es', ttl=120000):  # 加锁操作，120000毫秒/120秒失效
            not_downloaded_datas_list = get_not_downloaded_datas(indexs=indexs, query_id="es_query_not_download",
                                                                 vector_id_list=vector_id_list)
            # print(not_downloaded_datas_list)
            total_value = async_io_result(not_downloaded_datas_list, indexs=indexs)
            # print(total_value)
            for io_result in total_value:  # 3个io操作
                if io_result is False:
                    logger.error("内部服务io错误")
                    return False

            # 检查es操作是否完成，后续可接es基础服务的异步结果，目前不支持
            while longest_time > 0:
                time.sleep(1)
                longest_time -= 1
                not_download_rowkey_list = []  # 未使用的rowkey列表
                for i, data in enumerate(not_downloaded_datas_list):
                    rowkey = data.get("rowkey", "")
                    if rowkey:
                        not_download_rowkey_list.append(rowkey)
                not_download_rowkey_list_str = json.dumps(not_download_rowkey_list, ensure_ascii=False)
                check_dw_datas = get_batch_es_data(query_id="query_check_re_clean", paras=[not_download_rowkey_list_str],
                                                   indexs=indexs)
                if len(check_dw_datas) == 0:
                    return True # 执行成功
        return False # 超时执行失败，但实际不一定失败，es基础服务的结果，由于采用消息队列，一般不会丢，但积压可能产生版本异常
    status = False
    try:
        status = save_modify(vector_id_list=vector_id_list, indexs=indexs, longest_time=longest_time)
    except Exception as e:
        logger.exception(msg=e)
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


def async_io_result(params, indexs):
    """
    对查询的数据（含vector_id、rowkey、business_category字段）执行数据记录删除操作
    :param params: [{'rowkey': '020058828560700da7544f5f1de51b', 'vector_id': '395153', 'business_category': 'B2C'}, {'rowkey': '0200773838847028f290c8f76e44dd', 'vector_id': '459730', 'business_category': 'B2C'}]
    :return: [True, True, True]
    """
    total_value = [] # 异步返回数据
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        to_do = []
        business_category_to_vector_id_dict = {"B2B": [], "B2C": []} # vector_id列表
        for i, data in enumerate(params):
            vector_id = data.get("vector_id", "")
            business_category = data.get("business_category", "")
            if vector_id and business_category == "B2B":
                business_category_to_vector_id_dict["B2B"].append(int(vector_id))
            elif vector_id and business_category == "B2C":
                business_category_to_vector_id_dict["B2C"].append(int(vector_id))
        # 操作删除向量数据, 批量删除未使用文档对应的向量
        future1 = executor.submit(delete_milvus_vectors, mymilvus=mymilvus, vector_id_list=business_category_to_vector_id_dict["B2B"], business_category="B2B")
        to_do.append(future1)
        future2 = executor.submit(delete_milvus_vectors, mymilvus=mymilvus, vector_id_list=business_category_to_vector_id_dict["B2C"], business_category="B2C")
        to_do.append(future2)
        not_download_rowkey_list = [] # 未使用的rowkey列表
        for i, data in enumerate(params):
            rowkey = data.get("rowkey", "")
            if rowkey:
                not_download_rowkey_list.append(rowkey)
        not_download_rowkey_list_str = json.dumps(not_download_rowkey_list, ensure_ascii=False)
        # 应用库字段修改,未使用文档更新为已使用（用update_by_query方式）
        future3 = executor.submit(update_by_query_es_data, query_id="update_by_query_re_clean", paras=[not_download_rowkey_list_str], indexs=indexs)
        to_do.append(future3)

        for future in concurrent.futures.as_completed(to_do):
            res_future = future.result()
            total_value.append(res_future)
    return total_value


# 批量删除未使用文档对应的向量
@validate_arguments
def delete_milvus_vectors(mymilvus, vector_id_list: List[int], business_category: str) -> bool:
    """
    从数据库中删除指定id列表的向量，id列表不能为空
    """
    if not vector_id_list:
        return True
    if business_category == "B2B":
        collection_name = "qianci_baixing_content_b2b"
    elif business_category == "B2C":
        collection_name = "qianci_baixing_content_b2c"
    else:
        raise ValueError("business_category should be 'B2B' or 'B2C'")

    @retry(stop_max_attempt_number=3)
    def delete_milvus101(mymilvus, ids: List[int], collection_name: str=None) -> bool:
        status = mymilvus.delete_entity_by_id(collection_name=collection_name, vector_ids=ids)
        if status:
            logger.info("[数据批量删除] [Delete vectors successfully! Id are {}]".format(ids))
        else:
            raise Exception("delete vectors error")
        mymilvus.flush(collection_names=[collection_name]) # 落盘
        return status

    status = False
    try:
        status = delete_milvus101(mymilvus=mymilvus, ids=vector_id_list, collection_name=collection_name)
    except Exception as e:
        logger.error("[数据批量删除] [Delete vectors error! Id are {0}, error is {1}]".format(vector_id_list, e))
    return status


def update_by_query_es_data(query_id: str, paras: List, indexs: str = "dw_article") -> bool:
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


def get_required_fileds(es_datas: List) -> List:
    # 解析es字段为需要的格式字段，包括rowkey、vector_id、business_category。
    datas = []
    for i, data in enumerate(es_datas):
        element = {}
        element["rowkey"] = data["_source"].get("rowkey", "")
        element["vector_id"] = data["_source"].get("vector_id", "")
        element["business_category"] = data["_source"].get("business_category", "")
        datas.append(element)
    return datas


def get_not_downloaded_datas(indexs: str, query_id: str, vector_id_list: List[int]) -> List:
    """
    根据vector_id_list获取数据，并解析es字段为需要的格式字段列表。
    已使用包括下载次数大于0且is_used为0，未使用包括下载次数小于等于0.
    查询未使用文档的business_category，存在历史批次已改了is_used情况，所以只要download<=0即表示未使用
    上游认为有问题的数据用is_used字段标识，下游用status标识.
    :param indexs: es 索引名
    :param query_id: es 模板
    :param vector_id_list: 向量id列表
    :return:
    """
    vector_id_list_str_list = [str(i) for i in vector_id_list]
    vector_id_list_str = json.dumps(vector_id_list_str_list, ensure_ascii=False)
    dw_datas, scroll_id = get_batch_scroll_es_data(
        indexs=indexs, paras=[vector_id_list_str], query_id=query_id
    )
    dw_datas_all = get_required_fileds(dw_datas)  # 汇总游标筛选的数据，筛选的未使用的这部分数据

    while 1:
        if dw_datas:
            dw_datas, scroll_id = get_batch_scroll_es_data(
                indexs=indexs,
                paras=[vector_id_list_str],
                query_id=query_id,
                scroll_id=scroll_id,
            )
            dw_mini_batch = get_required_fileds(dw_datas)  # 得到id字段
            dw_datas_all.extend(dw_mini_batch)
        else:
            break
    logger.info("[二次清洗] [本批次应用库未使用文档数为{}]".format(len(dw_datas_all)))
    return dw_datas_all


def get_batch_scroll_es_data(indexs: str, query_id: str, paras: List, scroll_id: str = "") -> List:
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

if __name__ == "__main__":
    # rowkey_list = ["020058828560700da7544f5f1de51b", "0200773838847028f290c8f76e44dd"]
    # print(sync_check_es_update(rowkey_list, option="increase_download_count"))

    data = {"id_list": [395153, 459730]}
    vector_id_list = data.get("id_list")  # 对应应用库中vector_id
    sync_check_clean(vector_id_list=vector_id_list, indexs = "dw_article")



