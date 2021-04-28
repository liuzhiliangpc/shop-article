#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: celery_utils.py
@time: 2021/4/15 18:02
@desc: 该函数创建一个新的 Celery 对象，并用应用配置来配置中间人（Broker），
 用 Flask 配置更新其余的 Celery 配置，之后在应用上下文中创建一个封装任务执行的任务子类。
"""

from celery import Celery

def make_celery(app):
    celery = Celery(
        app.import_name,
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery