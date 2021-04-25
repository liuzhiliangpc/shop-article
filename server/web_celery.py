#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license:
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: web.py
@time: 2021/4/13 11:32
@desc: web服务，TODO 异步比想象的复杂
"""

import json
from tools.response import Response
from flask import request
from tools.log import logInit
from core.core import BackEndCore
from async_tasks.sync_re_clean_pipe import run as sync_re_clean_pipe_run

"----------------------------------------------------------"
logger = logInit('ALGO_BACKEND_WEB')
core = BackEndCore(web=True)
"-------------------------------------------------------------"
from init import app
from init import celery
from async_tasks.task1 import add_together

@app.route("/shop_article/sum/<arg1>/<arg2>", methods=['GET'])
def sum_(arg1, arg2):
    # 发送任务到celery,并返回任务ID,后续可以根据此任务ID获取任务结果
    result = add_together.delay(int(arg1), int(arg2))
    return result.id

@ app.route("/shop_article/get_result/<result_id>", methods=['GET'])
def get_result1(result_id):
    # 根据任务ID获取任务结果
    result = celery.AsyncResult(id=result_id)
    return str(result.get())

@app.route("/shop_article/sync_re_clean_request", methods=["POST"])
def sync_re_clean_request():
    infos = json.loads(request.get_data(as_text=True))
    logger.info(infos)
    try:
        result = sync_re_clean_pipe_run.delay(infos)
        return Response.success(result_id=result.id)
    except Exception as e:
        logger.error("任务id异常")
        return Response.error(msg='任务id异常')

@app.route("/shop_article/get_result", methods=["POST"])
def get_result():
    infos = json.loads(request.get_data(as_text=True))
    result_id = infos.get("result_id")
    try:
        result = celery.AsyncResult(id=result_id)
        return Response.success(result=str(result.get()))
    except Exception as e:
        logger.error("{} 异步数据异常".format(result_id))
        return Response.error(msg='异步结果异常')




"----------------------------------------------------------"
# 后台任务包括：执行添加清洗状态批量锁，查询当前未使用的文章，批量刷新待二次清洗的文章，删除重复数据库内的数据，
# 执行完成后解锁。

# # 二次清洗启动接口,实时接口
# @app.route("/shop_article/re_clean_request", methods=['POST'])
# def re_clean_request():
#
#     infos = json.loads(request.get_data(as_text=True))
#     try:
#         ret = core.handle('re_clean_pipe', infos)
#         if ret:
#             return Response.construct_response(ret)
#         else:
#             return Response.error(msg='error: parameter value "data" might cause some exceptions')
#     except Exception as e:
#         logger.error(e)
#         return Response.error(msg=str(e))


# # 应用库查询接口
# @app.route("/shop_article/query", methods=['POST'])
# def es_query():
#
#     infos = json.loads(request.get_data(as_text=True))
#     try:
#         ret = core.handle('query_string', infos)
#         if ret:
#             return Response.construct_response(ret)
#         else:
#             return Response.error(msg='error: parameter value "data" might cause some exceptions')
#     except Exception as e:
#         logger.error(e)
#         return Response.error(msg=str(e))
#
# # 应用库更新接口
# @app.route("/shop_article/update", methods=['POST'])
# def es_update():
#
#     infos = json.loads(request.get_data(as_text=True))
#     try:
#         ret = core.handle('update_by_query_es_fields', infos)
#         if ret:
#             return Response.construct_response(ret)
#         else:
#             return Response.error(msg='error: parameter value "data" might cause some exceptions')
#     except Exception as e:
#         logger.error(e)
#         return Response.error(msg=str(e))
#
# "======================================================================"


