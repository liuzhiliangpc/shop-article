#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: sync_upload_article.py
@time: 2021/9/8 14:33
@desc: 异步插入数据库
"""

import datetime
from init import celery
from retrying import retry
from typing import Any, Tuple, List, Dict  # 泛型类型支持
import json
import concurrent.futures
from core.core import logger
import hashlib
from tools.flask_postgresql_service import DBUtil
from config_outer import ConfigOuter
import copy
from tools.utils import db_execute
from math import isnan

db_util = DBUtil()


def generate_unique_id(url: str = "", published_time: str = "", content_s: str = "") -> str:
    """url+published_time+content_s唯一键md5值标识，为兼容自动爬虫，同步了其逻辑"""
    url = (url if url else "")
    published_time = (published_time if published_time else "")
    content = (content_s if content_s else "")
    row_key = hashlib.md5(json.dumps(url + published_time + content).encode("utf-8")).hexdigest().upper()
    return row_key


@celery.task
def sync_base_article_insert(request: Dict) -> bool:
    data = request.get("data", [])
    total_value = [] # 记录存储sql执行成功与否
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        to_do = []
        source_table = ConfigOuter.POSTGRESQL_MANUAL_ARTICLE_TABLE  # 文章素材表
        for index, value in enumerate(data):
            future = executor.submit(
                save_single_insert_data,
                source_table=source_table,
                row_key=value.get("row_key").strip(),
                language_s=value.get("language_s", "zh").strip(),
                url=value.get("url").strip(),
                crawler_time=value.get("crawler_time").strip(),
                title=value.get("title").strip(),
                content_s=value.get("content_s").strip(),
                site_name=value.get("site_name").strip(),
                category_s=value.get("category_s", "").strip(),
                level_s=value.get("level_s", "").strip(),
                industry_l1=value.get("industry_l1", "").strip(),
                industry_l2=value.get("industry_l2", "").strip(),
                industry_l3=value.get("industry_l3", "").strip(),
                crawler_batch_tag=value.get("crawler_batch_tag", "").strip(),
                business_category=value.get("business_category", "").strip(),
                crawler_industry_l1=value.get("crawler_industry_l1", "").strip(),
                crawler_industry_l2=value.get("crawler_industry_l2", "").strip(),
                crawler_industry_l3=value.get("crawler_industry_l3", "").strip(),
            )
            to_do.append(future)

        for future in concurrent.futures.as_completed(to_do):
            res_future = future.result()
            total_value.append(res_future)

        for res_code in total_value:
            if res_code == -1:
                logger.error("sql execute error")
                return False
    return True

def save_single_insert_data(source_table, **kwargs):
    """
    添加文章到表source_table中。
    :param source_table:
    :param kwargs:
    :return:
    """
    params = kwargs
    # row_key字段在此处生成较好
    row_key = generate_unique_id(url=params.get("url",""), published_time=params.get("published_time",""), content_s=params.get("content_s",""))
    params.update({"row_key": row_key})
    cols_str = ", ".join(params.keys())
    value_str = ", ".join([":{}".format(key) for key in params.keys()])
    execute_sql = """insert into {} ({}) values ({})""".format(source_table, cols_str, value_str)
    try:
        db_execute(db_util=db_util, sql=execute_sql, params=params, is_select=False)
        logger.info("[新增素材] [向表{}中插入数据 {}]".format(source_table, params))
    except Exception as e:
        logger.error("[新增素材] [向表{}中插入数据 {}失败:{}]".format(source_table, params))
        return -1 # 异常状态值
    return 0 # 正常返回值


# def save_single_insert_data(source_table: str, **kwargs) -> int:
#     """
#     添加词根到表source_table中，freq为0的软删除数据会被更新掉;
#     已存在的数据不会更新，需走更新接口; 不存在的数据会新增。
#     没有单独的操作记录表，所以采用软删除方式保留所有记录。
#     :param source_table: 词根表
#     :return: 0 表示正常；1 表示已存在一条正常词根，若有改动需使用更新函数；2表示内部该词根锁定不可用外部不可修改；-1表示SQL操作过程异常
#     """
#     params = kwargs
#     # 由于更新逻辑的存在，未避免保留旧的值，所以指定所有字段
#     select_sql = """select * from {} where word = :word""".format(source_table)
#     try:
#         df_data = db_execute(
#             db_util=db_util, sql=select_sql, params=params, is_select=True
#         )
#         logger.info("[词根表添加] [查找表{}是否存在词根{}]".format(source_table, params))
#     except Exception as e:
#         logger.error("[词根表添加] [查询表{}中是否有词根{}失败:{}]".format(source_table, params, e))
#         return -1  # 异常状态值
#     if df_data.shape[0] >= 1:
#         if df_data.loc[0, "status"] != 0:
#             return 2  # 数据内部锁定状态，该函数无法修改这条记录
#         elif df_data.loc[0, "freq"] > 0:
#             return 1  # 数据重复状态，此时不会被替换
#         else:
#             # 软删除的数据会更新掉全部字段，除了创建时间
#             set_value_dict = copy.copy(params)
#             set_value_dict.pop("word")  # 删除word字段
#             set_value_str = ", ".join(
#                 ["{} = :{}".format(key, key) for key in set_value_dict.keys()]
#             )
#             execute_sql = """update {} set {} where word = :word""".format(
#                 source_table, set_value_str
#             )
#     else:
#         # 新词根
#         create_value_dict = copy.copy(params)
#         if create_value_dict.get("freq") == 1:
#             create_value_dict.pop("freq")  # 待插入数据表字段删除freq字段，sql自动填充缺省值1
#         if not create_value_dict.get("status"):
#             create_value_dict.pop("status")  # 待插入数据表字段删除status字段，sql自动填充缺省值0
#
#         cols_str = ", ".join(create_value_dict.keys())
#         value_str = ", ".join([":{}".format(key) for key in create_value_dict.keys()])
#         execute_sql = """insert into {} ({}) values ({})""".format(
#             source_table, cols_str, value_str
#         )
#     try:
#         db_execute(db_util=db_util, sql=execute_sql, params=params, is_select=False)
#         logger.info("[词根表添加] [向表{}中添加/更新数据{}]".format(source_table, params))
#     except Exception as e:
#         logger.error("[词根表添加] [向表{}中添加/更新数据{}失败:{}]".format(source_table, params, e))
#         return -1  # 异常状态值
#     return 0  # 正常返回