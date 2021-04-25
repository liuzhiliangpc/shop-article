#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: response.py
@time: 2021/4/23 14:16
@desc: 自定义响应类
"""

import numpy as np
from flask import Response as FlaskResponse
import json
from typing import Dict # 泛型类型支持

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

# 常用返回结果
class Response:
    @staticmethod
    def construct_response(res_content: Dict) -> FlaskResponse:
        return FlaskResponse(json.dumps(res_content, cls=NpEncoder, ensure_ascii=False), mimetype='application/json') # 声明Content-Type为json格式

    ## 错误返回
    @staticmethod
    def error(msg: str='', **kwargs) -> FlaskResponse:
        res_content = {
            'code': 1,
            'msg': msg
        }
        res_content.update(kwargs)
        return Response.construct_response(res_content)

    ## 成功返回
    @staticmethod
    def success(msg: str = 'success', **kwargs) -> FlaskResponse:
        res_content = {
            'code': 0,
            'msg': msg,
        }
        res_content.update(kwargs)
        return Response.construct_response(res_content)

    @staticmethod
    def permission_denied():
        return Response.error(msg='没有操作权限')

