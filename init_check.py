#!/usr/bin/env python3
# encoding: utf-8
'''
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: init_check.py
@time: 2020/10/9 10:33
@desc: 模型的初始化，使得客户端调用时模型不存在加载延迟，目前加载延迟最大有3-5秒，会影响监控和用户体验
'''
import requests
import json
import time
from tools.log import logInit
from multiprocessing import Pool
from config import conf

ip = 'localhost'
port = conf.getConf('http_server', 'PORT', 'int')
logger = logInit('init_check')

def worker(**kwd):
    if kwd['option'] == 'duplicate_detection_history':
        value = get_duplicateDetection_history(data=kwd['data'])
    if kwd['option'] == 'duplicate_detection_realtime':
        value = get_duplicateDetection_realtime(data=kwd['data'])
    if kwd['option'] == 'insert':
        value = get_insert(data=kwd['data'])
    if kwd['option'] == 'delete':
        value = get_delete(data=kwd['data'])
    if kwd['option'] == 'npci':
        value = get_npci(data=kwd['data'])

## 地名识别
def get_npci(data):
    params = data
    r = requests.post(url='http://%s:%s/base_nlp/npci'%(ip, port), json=params, verify=False)
    ret = json.loads(r.text)
    relogits = ret.get('data', None)
    return relogits

## 数据插入接口，插入发布成功的数据，生成向量存入数据库
def get_insert(data):
    params = data
    r = requests.post(url='http://%s:%s/data_sync/insert'%(ip, port), json=params, verify=False)
    ret = json.loads(r.text)
    relogits = ret.get('data', None)
    return relogits

## 数据删除接口，删除指定id的数据
def get_delete(data):
    params = data
    r = requests.post(url='http://%s:%s/data_sync/delete'%(ip, port), json=params, verify=False)
    ret = json.loads(r.text)
    relogits = ret.get('data', None)
    return relogits

## 文本语义相似度（semantic textual similarity）全量重复度检测，允许一至多个问题/回答（现阶段主要针对百姓知道的问题）
def get_duplicateDetection_history(data):
    params = data
    r = requests.post(url='http://%s:%s/quality/duplicate_detection_history'%(ip, port), json=params, verify=False)
    ret = json.loads(r.text)
    relogits = ret.get('data', None)
    return relogits

## 文本语义相似度（semantic textual similarity）非全量重复度检测，允许多个问题/回答（现阶段主要针对百姓知道的回答）
def get_duplicateDetection_realtime(data):
    params = data
    r = requests.post(url='http://%s:%s/quality/duplicate_detection_realtime'%(ip, port), json=params, verify=False)
    ret = json.loads(r.text)
    relogits = ret.get('data', None)
    return relogits


def load_model():

    ## 自身重复度检查用例
    request_realtime = {
        "query": {
            "question": ["我是中国人", "中国人在哪儿"],
            "answer": ["我是中国人", "中国人在哪儿"]
        },
        "source": "zhidao_baixing"
    }
    ## 历史重复度检查用例
    request_history = {
        "query": {
            "question": [{
                "id": 123456,
                "text": "我是中国人"
            }],
            "answer": [{
                # "id": 123457,
                "id": "2f75d7896780ca8b12345678",
                "text": """看看美即新上市的鲜注膜力系列面膜哦。这款面膜采用“膜液分离”设计，而且精华液和膜布是分开存放的，补水锁水效果都很棒的。而且它用的是海藻纤维膜布，能够瞬间吸收20倍自身重量的精华液，使用的时候才把新鲜的精华液挤压到到膜布包装里，就可以了，很高级啊有木有。充分浸透后的面膜很透亮，摸起来又有弹性，推荐你试试~"""
            }
            ]
        },
        "source": "zhidao_baixing"
    }
    ## 批量插入用例
    request_insert = {
        "insert": {
            "question": [{
                "id": 123456,
                "text": "我是中国人"
            }],
            "answer": [{
                # "id": 123457,
                "id": "2f75d7896780ca8b12345678",
                "text": "中国人在哪儿"
            }
            ]
        },
        "date_time": "20200924",
        "source": "zhidao_baixing"
    }
    ## 批量删除用例
    request_delete = {
        'delete': {
            'question': [123456],
            # 'answer': [123457],
            'answer': ['2f75d7896780ca8b12345678']
        },
        'source': 'zhidao_baixing'
    }
    ## 地名识别用例
    request_npci = {
        "query": {
            "text": ["鼓楼北苑华贸城"],
        },
        "source": "zhidao_baixing"
    }

    # logger.info('初始化加载结束')
    # 3个进程初始化加载
    logger.info('异步初始化前')
    try:
        ps = Pool(3)
        ps.apply_async(worker, kwds={'option': 'duplicate_detection_realtime', 'data': request_realtime})
        ps.apply_async(worker, kwds={'option': 'duplicate_detection_history', 'data': request_history})
        ps.apply_async(worker, kwds={'option': 'npci', 'data': request_npci})
        ps.close()
        ps.join()
        logger.info('完成异步初始化')
    except Exception as e:
        logger.error('初始化检查失败')


if __name__ == '__main__':
    load_model()
