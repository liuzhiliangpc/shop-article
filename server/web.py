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
from flask import Flask, Response
from flask import request

from tools.log import logInit
from core.core import BackEndCore
import numpy as np
from config import conf
from celery import Celery
from celery.result import AsyncResult
import time
from pydantic import validate_arguments, ValidationError # 支持泛型参数校验
from typing import Any, Tuple, List, Dict # 泛型类型支持


# from flask import (make_response, jsonify,)
app = Flask(__name__)

mode=conf.getConf("common", "MODE")
if mode == "local":
    host = conf.getConf('redis', 'redis_host_local')
    port = conf.getConf('redis', 'redis_port_local', 'int')
    db = conf.getConf('redis', 'redis_db_local', 'int')
elif mode == "dev":
    host = conf.getConf('redis', 'redis_host_dev')
    port = conf.getConf('redis', 'redis_port_dev', 'int')
    db = conf.getConf('redis', 'redis_db_dev', 'int')
elif mode == "prod":
    host = conf.getConf('redis', 'redis_host_prod')
    port = conf.getConf('redis', 'redis_port_prod', 'int')
    db = conf.getConf('redis', 'redis_db_prod', 'int')

redis_url = f'redis://{host}:{port}/{db}'
# print(redis_url)
# 配置消息代理的路径，如果是在远程服务器上，则配置远程服务器中redis的URL
app.config['CELERY_BROKER_URL'] = redis_url  # 'redis://localhost:6379/0'
# 要存储 Celery 任务的状态或运行结果时就必须要配置
app.config['CELERY_RESULT_BACKEND'] = redis_url  # 'redis://localhost:6379/0'

logger = logInit('ALGO_BACKEND_WEB')
core = BackEndCore(web=True)

celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
TaskBase = celery.Task

@celery.task
@validate_arguments
def background_task(id_list: List) -> List:
    time.sleep(3)
    return id_list

# 后台任务包括：执行添加清洗状态批量锁，查询当前未使用的文章，批量刷新待二次清洗的文章，删除重复数据库内的数据，
# 执行完成后解锁。
@app.route("/shop_article/push_clean_ids", methods=['POST', 'GET'])
def push_clean_ids():
    if request.method == 'POST':
        # infos = request.values.to_dict()
        infos = json.loads(request.get_data(as_text=True))
    else:
        return HttpResponse({'retcode': 1, 'msg': 'web request must use method of post'})

    try:
        # ret = core.handle('get_re_clean_ids', infos)
        id_list = infos.get("id_list")
        result = background_task.delay(id_list) # result.id为string类型
        ret = {'retcode': 0, 'result_id': result.id, 'msg': 'ok'}
        if ret:
            return HttpResponse(ret)
        else:
            return HttpResponse({'retcode': 1, 'msg': 'error: parameter value "data" might cause some exceptions'})
    except Exception as e:
        logger.error(e)
        return HttpResponse({'retcode': 1, 'msg': "{}".format(e)})

@app.route("/shop_article/get_result", methods=['POST', 'GET'])
def get_result():
    if request.method == 'POST':
        # infos = request.values.to_dict()
        infos = json.loads(request.get_data(as_text=True))
    else:
        return HttpResponse({'retcode': 1, 'msg': 'web request must use method of post'})
    # 根据任务id获取任务结果
    result_id = infos.get("result_id")
    print(result_id)
    result = AsyncResult(id=result_id)
    print(result.get())
    ret = {'retcode': 0, 'result_info': str(result.get()), 'msg': 'ok'}
    return HttpResponse(ret)

## 自定义序列化方法
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)

def HttpResponse(data):
    result_info = data
    # return json.dumps(success_info, ensure_ascii=False)
    # return json.dumps(success_info, cls=NpEncoder, ensure_ascii=False)
    return Response(json.dumps(result_info, cls=NpEncoder, ensure_ascii=False), mimetype='application/json') # 声明Content-Type为json格式

# 获取店铺任务数据功能
@app.route("/shop_article/task_receive", methods=['POST', 'GET'])
def task_receive():
    if request.method == 'POST':
        # infos = request.values.to_dict()
        infos = json.loads(request.get_data(as_text=True))
    else:
        return HttpResponse({'retcode': 1, 'msg': 'web request must use method of post'})
    # print(infos)

    try:
        ret = core.handle('a0001', infos)
        if ret:
            return HttpResponse(ret)
        else:
            return HttpResponse({'retcode': 1, 'msg': 'error: parameter value "data" might cause some exceptions'})
    except Exception as e:
        logger.error(e)
        return HttpResponse({'retcode': 1, 'msg': "{}".format(e)})

# 素材请求功能
@app.route("/shop_article/article_request", methods=['POST', 'GET'])
def article_request():
    if request.method == 'POST':
        # infos = request.values.to_dict()
        infos = json.loads(request.get_data(as_text=True))
    else:
        return HttpResponse({'retcode': 1, 'msg': 'web request must use method of post'})
    # print(infos)

    try:
        ret = core.handle('a0002', infos)
        if ret:
            return HttpResponse(ret)
        else:
            return HttpResponse({'retcode': 1, 'msg': 'error: parameter value "data" might cause some exceptions'})
    except Exception as e:
        logger.error(e)
        return HttpResponse({'retcode': 1, 'msg': "{}".format(e)})

# 素材确认接收
@app.route("/shop_article/article_confirm", methods=['POST', 'GET'])
def article_confirm():
    if request.method == 'POST':
        # infos = request.values.to_dict()
        infos = json.loads(request.get_data(as_text=True))
    else:
        return HttpResponse({'retcode': 1, 'msg': 'web request must use method of post'})
    # print(infos)

    try:
        ret = core.handle('a0003', infos)
        if ret:
            return HttpResponse(ret)
        else:
            return HttpResponse({'retcode': 1, 'msg': 'error: parameter value "data" might cause some exceptions'})
    except Exception as e:
        logger.error(e)
        return HttpResponse({'retcode': 1, 'msg': "{}".format(e)})


# 二次清洗启动接口
@app.route("/shop_article/re_clean_request", methods=['POST', 'GET'])
def re_clean_request():
    if request.method == 'POST':
        infos = json.loads(request.get_data(as_text=True))
    else:
        return HttpResponse({'retcode': 1, 'msg': 'web request must use method of post'})

    try:
        ret = core.handle('re_clean_pipe', infos)
        if ret:
            return HttpResponse(ret)
        else:
            return HttpResponse({'retcode': 1, 'msg': 'error: parameter value "data" might cause some exceptions'})
    except Exception as e:
        logger.error(e)
        return HttpResponse({'retcode': 1, 'msg': "{}".format(e)})

# 应用库查询接口
@app.route("/shop_article/query", methods=['POST', 'GET'])
def es_query():
    if request.method == 'POST':
        infos = json.loads(request.get_data(as_text=True))
    else:
        return HttpResponse({'retcode': 1, 'msg': 'web request must use method of post'})

    try:
        ret = core.handle('query_string', infos)
        if ret:
            return HttpResponse(ret)
        else:
            return HttpResponse({'retcode': 1, 'msg': 'error: parameter value "data" might cause some exceptions'})
    except Exception as e:
        logger.error(e)
        return HttpResponse({'retcode': 1, 'msg': "{}".format(e)})

# 应用库更新接口
@app.route("/shop_article/update", methods=['POST', 'GET'])
def es_update():
    if request.method == 'POST':
        infos = json.loads(request.get_data(as_text=True))
    else:
        return HttpResponse({'retcode': 1, 'msg': 'web request must use method of post'})

    try:
        ret = core.handle('update_by_query_es_fields', infos)
        if ret:
            return HttpResponse(ret)
        else:
            return HttpResponse({'retcode': 1, 'msg': 'error: parameter value "data" might cause some exceptions'})
    except Exception as e:
        logger.error(e)
        return HttpResponse({'retcode': 1, 'msg': "{}".format(e)})

