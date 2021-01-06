#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license:
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: article_publish.py
@time: 2020/12/24 12:06
@desc:
"""
from core.core import logger
from tools.baixing_elasticsearch import BXElasticSearch
from retrying import retry
from tools.mypsycopg2 import Mypsycopg2

from config import conf

postgresql_mode = conf.getConf("postgresql", "mode")
source_table = "shop_task_dev" # 缺省值

if postgresql_mode == "prod_dev":
    source_table = conf.getConf("postgresql", "dev_shop_tasks")
elif postgresql_mode == "prod":
    source_table = conf.getConf("postgresql", "prod_shop_tasks")

es = BXElasticSearch()  # 百姓es实例

def get_batch_es_data(indexs, query_id, paras, max_nums=1000):
    """
    获取ES数据库数据，提供一批文档
    :param indexs: es 索引名
    :param query_id: es 模板
    :param paras: 参数列表
    :param max_nums: 最大条数
    """

    @retry(stop_max_attempt_number=3)
    def es_retry():
        es_back = None
        try:
            es_back = es.search_pro(
                query_id=query_id,
                paras=paras,
                indexs=indexs
            )
        except Exception as e:
            logger.exception(msg=e)
        return es_back

    es_back = es_retry()
    # print(es_back)
    es_response = es_back.get("restResponse")
    datas = []
    if es_response:
        count = es_response["hits"]["total"]["value"]
        if count > 0:
            datas = es_response["hits"]["hits"][:max_nums]  # 取匹配的若干条数据,最多为1000条数据
    return datas

def get_unfinished_tasks_table():
    # 获取未完成任务
    mypg = Mypsycopg2()  # pg实例
    query_shop_task_sql = """SELECT * FROM {} WHERE status <> 0 or status is NULL ORDER BY task_create_time ASC""".format(source_table)
    try:
        data_df = mypg.execute(query_shop_task_sql)
        logger.info("从表shop_task中获取未完成任务信息")
    except Exception as e:
        logger.error("执行sql语句,从表shop_tasks中获取未完成任务信息失败 {}".format(e))
    # # 关闭数据库连接
    mypg.close()
    logger.info("未完成任务总数为:{}".format(data_df.shape[0]))
    return data_df

def check_task_id(task_id):
    """
    检查当前任务id是否为合法的未完成任务id
    :param task_id: 任务id
    :return: 合法的id，返回True；否则，返回False
    """
    data_df = get_unfinished_tasks_table()  # 获取未完成的任务id
    task_ids_list = list(data_df["task_id"])
    if task_id in task_ids_list:
        return True
    return False

def get_article(indexs, task_id, request_nums):
    """
    获取发文文章
    :param indexs: 索引名
    :param task_id: 任务id
    :param request_nums: 请求数
    :return: data体和remain_now_nums
    """
    remain_now_nums = 0   # 剩余文档数目，含本次
    datas_not_used = []   # 未被使用的文档列表
    datas_response = []   # 返回给业务的文档列表 去除_source之外的字段
    #

    # 从新的dw_ai_article中，查询数据
    # 通过任务id查询得到的原始数据
    datas = get_batch_es_data(indexs=indexs, query_id="01002", paras=[task_id])  # 任务id查询
    retcode = 1 # 缺省值
    if not datas:
        if check_task_id(task_id=task_id):
            logger.info("文章准备中")
            retcode = 3
            msg = '文章准备中'
        else:
            logger.warning("非法任务，任务编号不存在")
            retcode = 4
            msg = '非法任务，任务编号不存在'
    else:
        for data in datas:
            data = data["_source"]
            if data.get("is_used", 0) == 0: # 未被使用的草稿箱文章
                remain_now_nums += 1 # 剩余的文档数目，含本次
                datas_not_used.append(data)

        if datas_not_used and len(datas_not_used) > request_nums:  # 存在未被使用文章
            datas_response = datas_not_used[:request_nums]
            logger.info("操作成功，本次请求后，发文任务有剩余文章额度")
            retcode = 0
            msg = '操作成功，本次请求后，发文任务有剩余文章额度'
        else:
            datas_response = datas_not_used  # 剩余的全部返回给业务
            logger.info("操作成功，本次请求后，发文任务无剩余文章额度")
            retcode = 2
            msg = '操作成功，本次请求后，发文任务无剩余文章额度'
        # 增删结果字段
        for data in datas_response:
            data["image_urls"] = data["image_oss_urls"] # 接口仅保留图片oss地址
            del data["image_oss_urls"] # 删除字段image_oss_urls,直接在原数据上操作
            del data["is_used"]  # 删除字段is_used
    return datas_response, remain_now_nums, retcode, msg


if __name__ == "__main__":
    pass
    # task_id = "123456"
    # batch_size = 3
    # print(get_article(indexs="dw_ai_article", task_id=task_id, request_nums=batch_size))
    # check_task_id()
    # print(get_article(indexs="dw_ai_article", task_id="999999", request_nums=1)[0])
