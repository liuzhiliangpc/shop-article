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
from retrying import retry
from pydantic import validate_arguments, ValidationError  # 支持泛型参数校验
from typing import Any, Tuple, List, Dict  # 泛型类型支持
import json
from tools.milvus_utils101 import MyMilvus
import concurrent.futures

mymilvus = MyMilvus()
es = BXElasticSearch()
logger = logInit("ElasticSearch && Milvus")
indexs = "dw_article"

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
        # es_back = es_retry()
        es_back = {"response":"success"} # TODO 此处为test，后面需要修改
        pass
    except Exception as e:
        logger.exception(msg=e)
    status = (True if es_back else False)
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
    res = False
    try:
        res = validate_request(re_clean_ids=re_clean_ids)
    except ValidationError as e:
        logger.error("data validation failed:{},{}".format(re_clean_ids, e))
    if not res:
        logger.error("data is none:{}".format(re_clean_ids))
        # raise ValueError("data is none:{}".format(re_clean_ids))
        r["retcode"] = 1
        r["msg"] = "data validation error"
    else:
        not_downloaded_datas_list = get_not_downloaded_datas(indexs=indexs, query_id="es_query_not_download",
                                                             vector_id_list=re_clean_ids)
        print(not_downloaded_datas_list)
        total_value = async_io_result(not_downloaded_datas_list)
        print(total_value)
        for io_result in total_value:
            if io_result is False:
                logger.error("服务端错误")
                r["retcode"] = 1
                r["msg"] = "server error"
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
    :param paras: 参数列表
    :param scroll_id: 游标id 首次或中途、最后提交都会有scrollId这个参数
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
            # print(dw_datas)
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


def async_io_result(params):
    """
    对查询的数据（含vector_id、rowkey、business_category字段）执行数据记录删除操作
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

if __name__ == "__main__":
    time0 = time.time()
    data = {"id_list": [395153, 459730]}
    print(run(request=data))
    # 顺序为先加锁，并根据id范围查询未使用的rowkey，然后异步删除对应的向量和es文档，完成后解锁退出
    # 缺少的异步环节是确认成功删除的查询，es和向量都要检查。
    # 可视为查询和删除、再确认删除成功是一个调度任务。
    print(time.time()- time0)