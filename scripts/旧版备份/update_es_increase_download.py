#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: update_es_increase_download.py
@time: 2021/4/13 18:32
@desc: download_count + 1操作，单次输入不超过10000条
和update_by_query二选一操作，TODO 暂时未完善。不可用，要独立出一个消耗文档接口
"""

import time
from tools.baixing_elasticsearch import BXElasticSearch
import json
from retrying import retry
from init import red_lock_factory # 应该锁住内部更新阶段，而不是查询web接口
# from init import es

es = BXElasticSearch()


def put_es_data(indexs, data_list, id_field):
    """
    插入es,数据匹配传入草稿箱
    :param indexs: 索引名
    :param data_list: 列表，批量插入，与dw_article中document相比，多了keyword_layout_tag字段\
    多了任务相关信息，唯一键修改为article_id
    :param id_field: 唯一键字段名
    :return:
    """
    @retry(stop_max_attempt_number=3)
    def es_retry():
        try:
            status = es.put_pro(indexs=indexs, data=data_list, id_field=id_field)
        except Exception as e:
            logger.exception(msg=e)
        return status
    status = es_retry()
    return status

def update_es_data(indexs, data_list, id_field):
    """
    成功插入到dw_ai_article后，更新dw_article中的字段download_count。
    :param indexs: 索引名
    :param data_list: 列表，批量更新
    :param id_field: 唯一键字段名
    :return:
    """
    @retry(stop_max_attempt_number=3)
    def es_retry():
        try:
            status = es.update_pro(indexs=indexs, data=data_list, id_field=id_field)
        except Exception as e:
            logger.exception(msg=e)
        return status
    status = es_retry()
    return status
# 获取文档返回
def get_batch_es_data(indexs, query_id, paras, max_nums=1000):
    """
    获取ES数据库数据，提供文档
    :param indexs: es 索引名
    :param query_id: es 模板
    :param paras: 参数列表
    :return:
    """
    es_back = es.search_pro(query_id=query_id, paras=paras, indexs=indexs)
    es_response = es_back.get("restResponse")
    datas = []
    if es_response:
        count = es_response["hits"]["total"]["value"]
        if count > 0:
            datas = es_response["hits"]["hits"][:max_nums]  # 取匹配的若干条数据,最多为1000条数据
    return datas

# 查询校验版本号部分
rowkey_list = ["011057791016700ea78a2917dc14df", "011055814958700e2c72fb44cb7e26", "0110604494847077c5bf53967ffad1", "01105703994270a1057bd88a3b0f81", "02106360611470a46940f671c80978"]
query_parm_list_str = json.dumps(rowkey_list, ensure_ascii=False)

dw_datas = get_batch_es_data(
    indexs="dw_article",
    query_id="query_before_update",
    paras=[query_parm_list_str, 1000],
)
# 注意："_seq_no":70242, "_primary_term":1,在"_source"外，"hit""hit"下

# 更新部分
# for

# 查询当前rowkey文档的版本号，校验

with red_lock_factory.create_lock(f'update_es', ttl=30000): # 加锁操作，30000毫秒/30秒失效
    time.sleep(1)
    print(1)