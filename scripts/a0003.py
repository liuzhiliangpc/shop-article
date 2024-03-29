#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license:
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: a0003.py
@time: 2020/11/4 21:34
@desc: 素材j接收确认
"""
from core.core import logger
from tools.baixing_elasticsearch import BXElasticSearch
from tools.log import logger
from retrying import retry

es = BXElasticSearch()


def get_es_data(indexs, query_id, paras):
    """
    获取ES数据库数据，提供一个文档
    :param indexs: es 索引名
    :param query_id: es 模板
    :param paras: 参数列表
    :return:
    """

    @retry(stop_max_attempt_number=3)
    def es_retry():
        es_back = None
        try:
            es_back = es.search_pro(query_id=query_id, paras=paras, indexs=indexs)
        except Exception as e:
            logger.exception(msg=e)
        return es_back

    es_back = es_retry()
    es_response = es_back.get("restResponse")
    datas = []
    if es_response:
        count = es_response["hits"]["total"]["value"]
        if count > 0:
            datas = es_response["hits"]["hits"][0]["_source"]  # 取匹配的第一条数据
    return datas


def run(request):
    """
    素材接收确认功能
    :param request: 请求参数
    :return: 校验结果
    """
    r = {}
    # 获取一条请求数据
    event = request
    # 请求字段检验
    task_id = event.get("task_id")
    data = event.get("data")
    if not task_id or not data:
        r["retcode"] = 1
        r["msg"] = "请求缺少参数"
        return r

    if not isinstance(event["task_id"], str):
        r["retcode"] = 6
        r["msg"] = "请检查数据类型"
        return r
    # 检查data字段里数据类型
    for dic in event["data"]:
        r = {}
        if not isinstance(dic["compound_words_id"], str) or not isinstance(
            dic["rowkey"], str
        ):
            r["retcode"] = 6
            r["msg"] = "请检查数据类型"
            return r

    # 检验组合词id是否有重复
    if len(list(set([dic["compound_words_id"] for dic in event["data"]]))) < len(
        [dic["compound_words_id"] for dic in event["data"]]
    ):
        r["retcode"] = 2
        r["msg"] = "组合词编号有重复"
        return r
    # 检验article_id是否有重复
    if len(list(set([dic["article_id"] for dic in event["data"]]))) < len(
        [dic["article_id"] for dic in event["data"]]
    ):
        r["retcode"] = 2
        r["msg"] = "article_id有重复"
        return r
    # 检验article_id编号是否存在
    for confirm_dict in event["data"]:
        back_info_dict = get_es_data(
            indexs="dw_ai_article", query_id="02001", paras=[confirm_dict["article_id"]]
        )
        if len(back_info_dict) == 0:
            r["retcode"] = 4
            r["msg"] = "article_id不存在"
            return r
        # 检验task_id是否存在
        if str(back_info_dict.get("task_id")) != event["task_id"]:
            r["retcode"] = 4
            r["msg"] = "task_id不存在"
            return r
    logger.info("[素材确认接收] [任务编号:{}]".format(str(event["task_id"])))

    r["msg"] = change_is_used_status("dw_ai_article", event["data"])
    if r["msg"] == "操作成功":
        r["retcode"] = 0
    else:
        r["retcode"] = 1
    logger.info("[素材接收返回] [结果:{}]".format(str(r)))
    return r


def change_is_used_status(indexs, data_list):
    """
    material_recieve 确认接收的组词编号与文章id，草稿箱内确认后的文章下次不会被请求到。
    :param indexs: es索引
    :param datalist: 为列表，包含待确认的文章信息
    :return:
    """
    # 更改 article_id is_used状态
    update_dw_article_datas = []
    for article_id in [dic["article_id"] for dic in data_list]:
        update_dw_article_datas.append({"is_used": 1, "article_id": article_id})

    flag_es = True  # 操作ES是否成功的标志
    for i in range(0, len(update_dw_article_datas), 50):
        update_dw_article_data_list = update_dw_article_datas[i : i + 50]
        status = update_es_data(
            indexs=indexs,
            data_list=update_dw_article_data_list,
            id_field="article_id",
        )  # 满足条件，则50条更新dw_ai_article
        if status.get("message") != "操作成功":
            flag_es = False
            break
    if flag_es:
        msg = "操作成功"
    else:
        msg = "es更新失败"
    return msg


def update_es_data(indexs, data_list, id_field):
    """
    更新dw_ai_article中的字段is_used。
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


if __name__ == "__main__":
    data = {
        "task_id": "18",
        "data": [
            {
                "compound_words_id": "190533",
                "rowkey": "0200749646847008ab6bc81e9e295e",
                "article_id": "02105385232200db17caf059f77c0e",
            }
        ],
    }
    run(request=data)
