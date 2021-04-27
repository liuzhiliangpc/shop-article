#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: init.py
@time: 2021/4/22 15:17
@desc: 初始化实例
"""

from flask import Flask
from tools.celery_utils import make_celery
from config_outer import ConfigOuter
import redis
# import redislite # 测试场景，pip安装，Redislite是Redis键值存储的自包含Python接口
from redlock import RedLockFactory

from tools.milvus_utils101 import MyMilvus
from tools.baixing_elasticsearch import BXElasticSearch


mymilvus = MyMilvus()
es = BXElasticSearch()
# redis连接池
redis_store = redis.StrictRedis(host=ConfigOuter.REDIS_LOCK_HOST, port=ConfigOuter.REDIS_LOCK_PORT, db=ConfigOuter.REDIS_LOCK_DB)
# 初始化阶段，实例化锁
red_lock_factory = RedLockFactory(connection_details=[redis_store])


def create_app():
    app = Flask(__name__)
    app.config.from_object(ConfigOuter)

    return app

app = create_app()
celery = make_celery(app)


