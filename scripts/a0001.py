#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: a0001.py
@time: 2020/11/4 21:34
@desc: 获取店铺任务数据
"""
import time
from tools.mypsycopg2 import Mypsycopg2
from core.core import logger
import json
from config import conf

source_table = conf.getConf("postgresql", "shop_tasks_source_table")  # 店铺发文信息表


def run(request):
    """
    校验请求功能
    :param request: 请求参数
    :return: 校验结果
    """
    r = {}
    # 获取一条请求数据
    event = request
    # 检验请求字段缺失
    task_id = event.get("task_id")
    task_create_time = event.get("task_create_time")
    business_category = event.get("business_category")
    industry_l2 = event.get("industry_l2")
    task_nums = event.get("task_nums")
    data = event.get("data")
    if (
        not task_id
        or not task_create_time
        or not business_category
        or not industry_l2
        or not task_nums
        or not data
    ):
        r["retcode"] = 1
        r["msg"] = "请求缺少参数"
        return r
    # 检查task_id、task_create_time、business_category、industry_l2的数据类型
    if (
        not isinstance(event["task_id"], str)
        or not isinstance(event["task_create_time"], str)
        or not isinstance(event["business_category"], str)
        or not isinstance(event["industry_l2"], str)
    ):
        r["retcode"] = 6
        r["msg"] = "参数数据类型错误"
        return r
    # 检查data字段里数据类型
    for dic in event["data"]:
        r = {}
        if (
            not isinstance(dic["compound_words_id"], str)
            or not isinstance(dic["compound_words_type"], str)
            or not isinstance(dic["compound_words"], str)
            or not isinstance(dic["root_A"], str)
            or not isinstance(dic["root_B"], str)
            or not isinstance(dic["root_C"], str)
            or not isinstance(dic["root_D"], str)
        ):
            r["retcode"] = 6
            r["msg"] = "参数数据类型错误"
            return r
    # 检查business_category
    if event["business_category"] not in ("B2B", "B2C"):
        r["retcode"] = 8
        r["msg"] = 'business_category 必须指定"B2B"或"B2C"'
        return r
    # 校验日期时间格式是否准确
    if not datetime_verify(event["task_create_time"]):
        r["retcode"] = 8
        r["msg"] = "task_create_time 格式不正确"
        return r
    # 检查data数量是否准确
    if len(event["data"]) != event["task_nums"]:
        r["retcode"] = 4
        r["msg"] = "data内容数量与 task_nums 数量不符"
        return r
    # 检查task_nums数量是否为在1~200之间
    if event["task_nums"] not in range(0, 201):
        r["retcode"] = 2
        r["msg"] = "发文任务超出单次额度范围"
        return r
    # TODO 检验组合词id是否有重复
    # if len(list(set([dic["compound_words_id"] for dic in event["data"]]))) < len(
    #     [dic["compound_words_id"] for dic in event["data"]]
    # ):
    #     r["retcode"] = 3
    #     r["msg"] = "组合词编号有重复"
    #     return r
    for i in range(event["task_nums"]):
        # 检查组合词中是否有AC
        if (
            "A" not in event["data"][i]["compound_words_type"]
            or "C" not in event["data"][i]["compound_words_type"]
        ):
            r["retcode"] = 7
            r["msg"] = "组合词必备AC"
            return r
        # 检查组合词类型是否一致
        for n in event["data"][i]["compound_words_type"]:
            root_key = "root_{}".format(n)
            if event["data"][i][root_key] == "":
                r["retcode"] = 5
                r["msg"] = "请求体中存在除root_B、root_D外，字段为空的情形"
                return r
        # 检查组合词是否缺词
        if (
            event["data"][i]["root_A"]
            + event["data"][i]["root_B"]
            + event["data"][i]["root_C"]
            + event["data"][i]["root_D"]
        ) != event["data"][i]["compound_words"]:
            r["retcode"] = 7
            r["msg"] = "词根组合与组合词不符"
            return r
    logger.info("[获取店铺任务数据] [接收数据:{}条]".format(str(event["task_nums"])))
    msg = save_request_data(source_table, event)
    if msg == "操作成功":
        r["retcode"] = 0
        r["msg"] = msg
    else:
        r["retcode"] = 1
        r["msg"] = msg
    logger.info("[请求校验] [结果:{}]".format(str(r)))
    return r


# 校验日期时间字符串是否正确
def datetime_verify(date):
    """判断是否是一个有效的日期时间字符串"""
    try:
        if ":" in date:
            time.strptime(date, "%Y-%m-%d %H:%M:%S")
        else:
            return False
        return True
    except Exception as e:
        print(e)
        return False


# 保存数据至数据库
def save_request_data(source_table, data):
    mypg = Mypsycopg2()
    params = data
    params["shop_task_json"] = json.dumps(data, ensure_ascii=False)
    params["status"] = 1
    insert_sql = """INSERT INTO {} (task_id, task_create_time, business_category, task_nums, shop_task_json, status, 
    industry_l2) VALUES (%(task_id)s, %(task_create_time)s, %(business_category)s, %(task_nums)s, %(shop_task_json)s, %(status)s, 
    %(industry_l2)s)""".format(
        source_table
    )
    try:
        mypg.execute(insert_sql, params)
        logger.info("[获取店铺任务数据] [向表{}中插入数据{}]".format(source_table, params))
        msg = "操作成功"
    except Exception as e:
        logger.error(
            "[获取店铺任务数据] [执行sql语句,向表{}中插入数据{}失败 {}]".format(source_table, params, e)
        )
        msg = "插入数据库失败"
    mypg.close()
    return msg


if __name__ == "__main__":
    d = list(
        [
            {
                "compound_words_id": "777",  # 组合词id
                "compound_words_type": "ABC",  # 组合词类型
                "compound_words": "上海刑事律师服务",  # 组合词
                "root_A": "上海",  # 词根A,缺省为空字符串""
                "root_B": "刑事",  # 词根B
                "root_C": "律师服务",  # 词根C
                "root_D": "",  # 词根D
            },
            {
                "compound_words_id": "666",  # 组合词id
                "compound_words_type": "ABC",  # 组合词类型
                "compound_words": "山东刑事律师服务",  # 组合词
                "root_A": "山东",  # 词根A,缺省为空字符串""
                "root_B": "刑事",  # 词根B
                "root_C": "律师服务",  # 词根C
                "root_D": "",  # 词根D
            },
        ]
    )
    data = {
        "task_id": "18112",  # 任务编号id
        "task_create_time": "2020-12-08 10:03:21",  # 任务创建时间
        "business_category": "B2B",  # 任务为B2B类型还是B2C类型（本地服务）
        "industry_l2": "律师服务",
        "data": d,
        "task_nums": 2,  # 总的任务请求素材数
    }
    print(run(data))
