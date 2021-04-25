#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: lock_with_redis.py
@time: 2021/4/15 12:00
@desc:
"""
import time
import redis
# import redislite # 测试场景，pip安装，Redislite是Redis键值存储的自包含Python接口
from redlock import RedLockFactory
from config_outer import Config
# redis连接池
redis_store = redis.StrictRedis(host=Config.REDIS_LOCK_HOST, port=Config.REDIS_LOCK_PORT, db=Config.REDIS_LOCK_DB)

print(redis_store)

# 初始化阶段，实例化锁
red_lock_factory = RedLockFactory(connection_details=[redis_store])

with red_lock_factory.create_lock(f'update_es', ttl=30000): # 加锁操作，30000毫秒/30秒失效
    time.sleep(1)
    print(1)

