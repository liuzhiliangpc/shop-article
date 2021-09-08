#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license:
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: web_celery.py
@time: 2021/4/27 11:32
@desc: web服务
"""

import json
from tools.response import Response
from flask import request
from core.core import BackEndCore, logger
from async_tasks.task3 import run

from async_tasks import sync_check_es_update # 合并后的接口
from async_tasks import sync_check_clean
from async_tasks import sync_base_article_insert
"----------------------------------------------------------"
from init import app
from init import celery
from async_tasks.task1 import add_together
"-------------------------------------------------------------"
core = BackEndCore(web=True)

@app.route("/shop_article/sum/<arg1>/<arg2>", methods=['GET'])
def sum_(arg1, arg2):
    # 发送任务到celery,并返回任务ID,后续可以根据此任务ID获取任务结果
    result = add_together.delay(int(arg1), int(arg2))
    return result.id


@app.route("/shop_article/task3_request", methods=["POST"])
def sync_re_clean_request():
    infos = json.loads(request.get_data(as_text=True))
    logger.info(infos)
    try:
        result = run.delay(infos)
        return Response.success(result_id=result.id)
        # result = run(infos)
        # return Response.success(result_id=result) # 改为同步接口
    except Exception as e:
        logger.error("任务id异常")
        return Response.error(msg='任务id异常')


@ app.route("/shop_article/get_result/<result_id>", methods=['GET'])
def get_result(result_id):
    # 根据任务ID获取任务结果
    logger.info({"result_id": result_id})
    try:
        result = celery.AsyncResult(id=result_id)
        return Response.success(result=result.get())
    except Exception as e:
        logger.error("{} 异步结果异常".format(result_id))
        return Response.error(msg='异步结果异常')


@app.route("/shop_article/get_result", methods=["POST"])
def post_result():
    infos = json.loads(request.get_data(as_text=True))
    logger.info(infos)
    result_id = infos.get("result_id")
    try:
        result = celery.AsyncResult(id=result_id)
        return Response.success(result=result.get())
    except Exception as e:
        logger.error("{} 异步结果异常".format(result_id))
        return Response.error(msg='异步结果异常')


# # 应用库查询接口
@app.route("/shop_article/query", methods=['POST'])
def es_query():
    infos = json.loads(request.get_data(as_text=True))
    logger.info(infos)
    try:
        ret = core.handle('query_string', infos)
        if ret:
            return Response.construct_response(ret)
        else:
            return Response.error(msg='error: parameter value "data" might cause some exceptions')
    except Exception as e:
        logger.error(e)
        return Response.error(msg=str(e))

"----------------------------------------------------------"

@app.route("/shop_article/increase_download_count", methods=["POST"])
def sync_increase_download_count():
    infos = json.loads(request.get_data(as_text=True))
    logger.info(infos)
    try:
        # 执行同步计算
        ret, match_rowkey_list = core.handle('update_es_increase_download', infos)
        if ret.get("retcode") == 0 and match_rowkey_list:
            result = sync_check_es_update.delay(rowkey_list=match_rowkey_list, option="increase_download_count")
            ret.update({"result_id": result.id})
            # result = sync_check_es_update.run(rowkey_list=match_rowkey_list, option="increase_download_count")
            # ret.update({"result_id": result})
        return Response.construct_response(ret)
    except Exception as e:
        logger.error("任务异常")
        return Response.error(msg=str(e))

# 应用库更新接口
@app.route("/shop_article/update", methods=['POST'])
def es_update():
    infos = json.loads(request.get_data(as_text=True))
    logger.info(infos)
    try:
        ret, match_rowkey_list = core.handle('update_es_fields', infos)
        if ret.get("retcode") == 0 and match_rowkey_list:
            option = infos.get("option")
            result = sync_check_es_update.delay(rowkey_list=match_rowkey_list, option=option)
            ret.update({"result_id": result.id})
            # result = sync_check_es_update.run(rowkey_list=match_rowkey_list, option=option)
            # ret.update({"result_id": result})
        return Response.construct_response(ret)
    except Exception as e:
        logger.error("任务异常")
        return Response.error(msg=str(e))

# 后台任务包括：执行添加清洗状态批量锁，查询当前未使用的文章，批量刷新待二次清洗的文章，删除重复数据库内的数据，执行完成后解锁。
# 二次清洗启动接口
@app.route("/shop_article/re_clean_request", methods=['POST'])
def re_clean_request():
    infos = json.loads(request.get_data(as_text=True))
    logger.info(infos)
    try:
        ret = core.handle('re_clean_pipe', infos)
        if ret.get("retcode") == 0:
            vector_id_list = infos.get("id_list")
            result = sync_check_clean.delay(vector_id_list=vector_id_list)
            ret.update({"result_id": result.id})
            # result = sync_check_clean.run(vector_id_list=vector_id_list)
            # ret.update({"result_id": result})
        return Response.construct_response(ret)
    except Exception as e:
        logger.error("任务异常")
        return Response.error(msg=str(e))

# "======================================================================"
# 基础文章异步导入到基础库
@app.route("/shop_article/base_article_insert", methods=["POST"])
def base_article_insert():
    infos = json.loads(request.get_data(as_text=True))
    try:
        # 执行同步计算
        ret = core.handle("base_article_insert", infos)
        if ret.get("retcode") == 0:
            result = sync_base_article_insert.delay(request=infos)
            ret.update({"result_id": result.id})
        return Response.construct_response(ret)
    except Exception as e:
        logger.error("任务异常")
        return Response.error(msg=str(e))