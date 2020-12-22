#! /usr/bin/env python3
#coding=utf-8
"""

    web服务

"""
import json
# import importlib

from flask import Flask, Response
from flask import request

from tools.log import logInit
from core.core import BackEndCore
import numpy as np
# from flask import (make_response, jsonify,)
app = Flask(__name__)

logger = logInit('ALGO_BACKEND_WEB')
core = BackEndCore(web=True)

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
